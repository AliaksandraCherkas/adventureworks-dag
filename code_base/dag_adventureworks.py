from __future__ import annotations
from datetime import datetime
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# --- Configuration Section ---
# Central place for all pipeline-specific constants. This makes the DAG easier to read and maintain.
GCP_PROJECT_ID = "adventureworks-project-466602"
GCP_REGION = "us-central1"
CLUSTER_NAME = "composer-adventureworks"
# The service account that the Dataproc cluster will use. It needs permissions for GCS, Dataproc, and Secret Manager.
DATAPROC_SA = "sa-dataproc@adventureworks-project-466602.iam.gserviceaccount.com"
# The main GCS bucket for storing scripts, data, and temporary files.
GCS_BUCKET = "bct-base-adventureworks"
# The GCS bucket where this DAG file and its dependencies are stored.
DAG_BUCKET = "dags-bucket01"
# The path within the main GCS bucket where JAR files are stored.
JAR_PATH = f"gs://{GCS_BUCKET}/scripts/jars"


# --- Dataproc Cluster Definition ---
# This dictionary defines the configuration for the temporary Dataproc cluster that will be created for the pipeline.
CLUSTER_CONFIG = {
    # Configuration for the primary (master) node.
    "master_config": {
        "num_instances": 1, 
        "machine_type_uri": "n1-standard-2", # A small machine type, suitable for orchestration.
        "disk_config": {"boot_disk_size_gb": 100},
    },
    # Configuration for the worker nodes.
    "worker_config": {
        "num_instances": 0, # Currently a single-node cluster. See recommendations below.
        "disk_config": {"boot_disk_size_gb": 50},
    },
    # General Google Compute Engine settings for the cluster VMs.
    "gce_cluster_config": {
        "service_account": DATAPROC_SA,
    },
    # Initialization actions are scripts that run on each node when the cluster is created.
    # This is perfect for installing dependencies or setting up services like the Cloud SQL Proxy.
    "initialization_actions": [
        {"executable_file": f"gs://{DAG_BUCKET}/dags/code_base/install_dependencies.sh"},
        {"executable_file": f"gs://{DAG_BUCKET}/dags/code_base/start-proxy.sh"},
    ],
}

# --- PySpark Job Definitions ---
# Defines the first job: ingesting data from Cloud SQL to GCS as Parquet.
JOB_1_SQL_TO_PARQUET = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/code_base/db_to_parquet.py",
        # This job requires the PostgreSQL JDBC driver to connect to the database.
        "jar_file_uris": [f"{JAR_PATH}/postgresql-42.7.7.jar"],
    },
}

# Defines the second job: transforming the Parquet files and loading the results into BigQuery.
JOB_2_PARQUET_TO_BQ = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/code_base/transform_to_bq.py",
    },
}


# --- DAG Definition ---
with DAG(
    dag_id="adventureworks_etl_pipeline",
    # Defines how often the DAG runs. 'None' means it only runs when triggered manually or by an external event.
    schedule=None,
    # The date on which the DAG should first start running.
    start_date=datetime(2024, 1, 1),
    # If set to True, Airflow would try to run for all missed schedules since the start_date. False is best practice for ETL DAGs.
    catchup=False,
    tags=["adventure_etl_works"],
) as dag:
    # This task creates the temporary Dataproc cluster using the CLUSTER_CONFIG defined above.
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
    )

    # This task submits the first PySpark job to the cluster. It will wait for the job to complete.
    submit_job_1_sql_to_parquet = DataprocSubmitJobOperator(
        task_id="submit_job_1_sql_to_parquet", job=JOB_1_SQL_TO_PARQUET, region=GCP_REGION, project_id=GCP_PROJECT_ID
    )

    # This task submits the second PySpark job. It only runs if the first job succeeds.
    submit_job_2_parquet_to_bq = DataprocSubmitJobOperator(
        task_id="submit_job_2_parquet_to_bq", job=JOB_2_PARQUET_TO_BQ, region=GCP_REGION, project_id=GCP_PROJECT_ID
    )

    # This task deletes the Dataproc cluster to save costs.
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        # The trigger_rule ensures this task runs regardless of whether the preceding tasks succeeded or failed.
        # This is CRITICAL for cost management to prevent orphaned clusters.
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define the workflow sequence using bitshift operators.
    # The pipeline will execute in this order: create cluster -> run job 1 -> run job 2 -> delete cluster.
    create_cluster >> submit_job_1_sql_to_parquet >> submit_job_2_parquet_to_bq >> delete_cluster