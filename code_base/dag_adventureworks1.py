from __future__ import annotations

from airflow.models.dag import DAG
# Import the Variable model
from airflow.models.variable import Variable
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

# --- Read Variables directly from Airflow DB using Python ---
# This happens when the DAG is parsed.
# The .strip() method is added as a safety measure to remove any accidental whitespace.
gcp_project_id = Variable.get("gcp_project_id").strip()
gcp_region = Variable.get("gcp_region").strip()
gcs_bucket = Variable.get("gcs_bucket").strip()
dataproc_cluster_name = Variable.get("dataproc_cluster_name").strip()
dataproc_service_account = Variable.get("dataproc_service_account").strip()
bq_dataset = Variable.get("bq_dataset").strip()
cloud_sql_instance_connection_name = Variable.get("cloud_sql_instance_connection_name").strip()
secret_id_db_user = Variable.get("secret_id_db_user").strip()
secret_id_db_pass = Variable.get("secret_id_db_pass").strip()
secret_id_db_name = Variable.get("secret_id_db_name").strip()


# --- Configuration Dictionaries using Python f-strings ---
# We no longer use Jinja templates {{...}} here. We use Python variables directly.

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
        "service_account": dataproc_service_account,
        "metadata": {
            "cloud-sql-instance-name": cloud_sql_instance_connection_name
        }
    },
    "initialization_actions": [
        {"executable_file": f"gs://{gcs_bucket}/scripts/start-proxy.sh"},
    ],
}

JOB_1_SQL_TO_PARQUET = {
    "reference": {"project_id": gcp_project_id},
    "placement": {"cluster_name": dataproc_cluster_name},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{gcs_bucket}/scripts/db_to_parquet.py",
        "jar_file_uris": [f"gs://{gcs_bucket}/scripts/jars/postgresql-42.7.7.jar"],
        "properties": {
            "spark.yarn.appMasterEnv.GCP_PROJECT_ID": gcp_project_id,
            "spark.yarn.appMasterEnv.GCS_OUTPUT_BUCKET": gcs_bucket,
            "spark.yarn.appMasterEnv.SECRET_ID_DB_USER": secret_id_db_user,
            "spark.yarn.appMasterEnv.SECRET_ID_DB_PASS": secret_id_db_pass,
            "spark.yarn.appMasterEnv.SECRET_ID_DB_NAME": secret_id_db_name,
            "spark.executorEnv.GCP_PROJECT_ID": gcp_project_id,
            "spark.executorEnv.GCS_OUTPUT_BUCKET": gcs_bucket,
            "spark.executorEnv.SECRET_ID_DB_USER": secret_id_db_user,
            "spark.executorEnv.SECRET_ID_DB_PASS": secret_id_db_pass,
            "spark.executorEnv.SECRET_ID_DB_NAME": secret_id_db_name,
        },
    },
}

JOB_2_PARQUET_TO_BQ = {
    "reference": {"project_id": gcp_project_id},
    "placement": {"cluster_name": dataproc_cluster_name},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{gcs_bucket}/scripts/transform_to_bq.py",
        "properties": {
            "spark.yarn.appMasterEnv.GCP_PROJECT_ID": gcp_project_id,
            "spark.yarn.appMasterEnv.GCS_BUCKET": gcs_bucket,
            "spark.yarn.appMasterEnv.BQ_DATASET": bq_dataset,
            "spark.executorEnv.GCP_PROJECT_ID": gcp_project_id,
            "spark.executorEnv.GCS_BUCKET": gcs_bucket,
            "spark.executorEnv.BQ_DATASET": bq_dataset,
        },
    },
}


with DAG(
    dag_id="adventureworks_etl_pipeline_v2", # Changed name to avoid conflicts
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["adventure_works", "v2"],
    doc_md="""### AdventureWorks ETL Pipeline (Direct Variable Access)""",
) as dag:
    # Operators no longer need Jinja templating as the dicts are pre-formatted
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=gcp_project_id,
        region=gcp_region,
        cluster_name=dataproc_cluster_name,
        cluster_config=CLUSTER_CONFIG,
    )

    submit_job_1_sql_to_parquet = DataprocSubmitJobOperator(
        task_id="submit_job_1_sql_to_parquet",
        project_id=gcp_project_id,
        region=gcp_region,
        job=JOB_1_SQL_TO_PARQUET,
    )

    submit_job_2_parquet_to_bq = DataprocSubmitJobOperator(
        task_id="submit_job_2_parquet_to_bq",
        project_id=gcp_project_id,
        region=gcp_region,
        job=JOB_2_PARQUET_TO_BQ,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=gcp_project_id,
        region=gcp_region,
        cluster_name=dataproc_cluster_name,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> submit_job_1_sql_to_parquet >> submit_job_2_parquet_to_bq >> delete_cluster