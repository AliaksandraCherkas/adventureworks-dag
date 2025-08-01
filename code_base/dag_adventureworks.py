from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# --- Configuration ---
GCP_PROJECT_ID = "adventureworks-project-466602"
GCP_REGION = "us-central1"
CLUSTER_NAME = "composer-adventureworks"
DATAPROC_SA = "sa-dataproc@adventureworks-project-466602.iam.gserviceaccount.com"
GCS_BUCKET = "bct-base-adventureworks"


# --- Cluster Definition ---
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
        "service_account": DATAPROC_SA,
    },
    "initialization_actions": [
        {"executable_file": f"gs://{GCS_BUCKET}/scripts/install_dependencies.sh"},
        {"executable_file": f"gs://{GCS_BUCKET}/scripts/start-proxy.sh"},
    ],
}

# --- Job Definitions ---
JOB_1_SQL_TO_PARQUET = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/db_to_parquet.py",
        "jar_file_uris": [f"gs://{GCS_BUCKET}/scripts/jars/postgresql-42.7.7.jar"],
    },
}

JOB_2_PARQUET_TO_BQ = {
    "reference": {"project_id": GCP_PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/scripts/transform_to_bq.py",
    },
}


with DAG(
    dag_id="adventureworks_etl_pipe",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["adventure_works"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_job_1_sql_to_parquet = DataprocSubmitJobOperator(
        task_id="submit_job_1_sql_to_parquet", job=JOB_1_SQL_TO_PARQUET, region=GCP_REGION, project_id=GCP_PROJECT_ID
    )

    submit_job_2_parquet_to_bq = DataprocSubmitJobOperator(
        task_id="submit_job_2_parquet_to_bq", job=JOB_2_PARQUET_TO_BQ, region=GCP_REGION, project_id=GCP_PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
    )

    # Define the workflow sequence
    create_cluster >> submit_job_1_sql_to_parquet >> submit_job_2_parquet_to_bq >> delete_cluster