from __future__ import annotations

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# --- Configuration Dictionaries using Jinja Templating ---
# Airflow will substitute the values from Airflow Variables at runtime.

# Dataproc Cluster Configuration
# All values that might change between environments are templated.
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 0,
        "disk_config": {"boot_disk_size_gb": 50},
    },
    "gce_cluster_config": {
        "service_account": "{{ var.value.dataproc_service_account }}",
        "metadata": {
            "cloud-sql-instance-name": "{{ var.value.cloud_sql_instance_connection_name }}"
        }
    },
    "initialization_actions": [
        {"executable_file": "gs://{{ var.value.gcs_bucket }}/scripts/install_dependencies.sh"},
        {"executable_file": "gs://{{ var.value.gcs_bucket }}/scripts/start-proxy1.sh"},
        {"executable_file": "gs://{{ var.value.gcs_bucket }}/scripts/whoami.sh"},
    ],
}

# Job Definition 1: Ingest from SQL Database to Parquet files in GCS
JOB_1_SQL_TO_PARQUET = {
    "reference": {"project_id": "{{ var.value.gcp_project_id }}"},
    "placement": {"cluster_name": "{{ var.value.dataproc_cluster_name }}"},
    "pyspark_job": {
        "main_python_file_uri": "gs://{{ var.value.gcs_bucket }}/scripts/code_base/db_to_parquet1.py",
        "jar_file_uris": [
            "gs://{{ var.value.gcs_bucket }}/scripts/jars/postgresql-42.7.7.jar"
        ],
        # --- THIS PROPERTIES BLOCK HAS BEEN ADDED ---
        "properties": {
            # Pass all environment variables the script needs to run.
            "spark.yarn.appMasterEnv.GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "spark.yarn.appMasterEnv.GCS_OUTPUT_BUCKET": "{{ var.value.gcs_bucket }}",
            "spark.yarn.appMasterEnv.SECRET_ID_DB_USER": "{{ var.value.secret_id_db_user }}",
            "spark.yarn.appMasterEnv.SECRET_ID_DB_PASS": "{{ var.value.secret_id_db_pass }}",
            "spark.yarn.appMasterEnv.SECRET_ID_DB_NAME": "{{ var.value.secret_id_db_name }}",

        },
    },
}

# Job Definition 2: Transform Parquet and Load to BigQuery
# IMPORTANT: 'properties' section added to pass environment variables
# to the second PySpark script (transform_to_bq.py).
JOB_2_PARQUET_TO_BQ = {
    "reference": {"project_id": "{{ var.value.gcp_project_id }}"},
    "placement": {"cluster_name": "{{ var.value.dataproc_cluster_name }}"},
    "pyspark_job": {
        "main_python_file_uri": "gs://{{ var.value.gcs_bucket }}/scripts/code_base/transform_to_bq1.py",
        "properties": {
            # These variables will be available in the PySpark script via os.getenv()
            "spark.yarn.appMasterEnv.GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "spark.yarn.appMasterEnv.GCS_BUCKET": "{{ var.value.gcs_bucket }}",
            "spark.yarn.appMasterEnv.BQ_DATASET": "{{ var.value.bq_dataset }}",
            "spark.executorEnv.GCP_PROJECT_ID": "{{ var.value.gcp_project_id }}",
            "spark.executorEnv.GCS_BUCKET": "{{ var.value.gcs_bucket }}",
            "spark.executorEnv.BQ_DATASET": "{{ var.value.bq_dataset }}",
        },
    },
}


with DAG(
    dag_id="adventureworks_etl_pipeline",
    # Using a standard, non-triggered schedule
    schedule=None,
    # Using a standard start_date method
    start_date=days_ago(1),
    catchup=False,
    tags=["adventure_works", "production"],
    # IMPORTANT: This allows Airflow to render Jinja templates inside Python objects (dicts, lists)
    render_template_as_native_obj=True,
    doc_md="""
    ### AdventureWorks ETL Pipeline

    This DAG orchestrates a complete ETL process using a temporary Dataproc cluster.

    **Workflow:**
    1.  **Creates** a temporary Dataproc cluster.
    2.  **Submits** a PySpark job to extract data from a SQL database and save it as Parquet in GCS.
    3.  **Submits** a second PySpark job to transform the Parquet data and load it into a BigQuery data warehouse.
    4.  **Deletes** the Dataproc cluster upon completion or failure of the main tasks.

    **Required Airflow Variables:**
    - `gcp_project_id`
    - `gcp_region`
    - `gcs_bucket`
    - `dataproc_cluster_name`
    - `dataproc_service_account`
    - `bq_dataset`
    """,
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        # Jinja templates are used to populate operator parameters from Airflow Variables
        project_id="{{ var.value.gcp_project_id }}",
        region="{{ var.value.gcp_region }}",
        cluster_name="{{ var.value.dataproc_cluster_name }}",
        # The cluster_config dict, containing templates, will be rendered by Airflow
        cluster_config=CLUSTER_CONFIG,
    )

    submit_job_1_sql_to_parquet = DataprocSubmitJobOperator(
        task_id="submit_job_1_sql_to_parquet",
        project_id="{{ var.value.gcp_project_id }}",
        region="{{ var.value.gcp_region }}",
        job=JOB_1_SQL_TO_PARQUET,
    )

    submit_job_2_parquet_to_bq = DataprocSubmitJobOperator(
        task_id="submit_job_2_parquet_to_bq",
        project_id="{{ var.value.gcp_project_id }}",
        region="{{ var.value.gcp_region }}",
        job=JOB_2_PARQUET_TO_BQ,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id="{{ var.value.gcp_project_id }}",
        region="{{ var.value.gcp_region }}",
        cluster_name="{{ var.value.dataproc_cluster_name }}",
        # This trigger rule ensures the cluster is always deleted, even if a job fails.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define the workflow sequence using bitshift operators
    create_cluster >> submit_job_1_sql_to_parquet >> submit_job_2_parquet_to_bq >> delete_cluster