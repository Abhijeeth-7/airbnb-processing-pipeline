from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
)
from datetime import datetime
import ast


def get_full_gcs_path(bucket_name, file_path, file_name):
    return f"gs://{bucket_name}/{file_path}/{file_name}"


# gcp buckets config
GCS_BUCKET_NAME = Variable.get("bucket_name")

SOURCE_DIR_PATH = "landing-zone/incoming-bookings-data"
SOURCE_FILE_NAME = f"bookings-{datetime.now().strftime('%Y-%m-%d')}.csv"

ARCHIVE_DIR_PATH = f"landing-zone/archive"
ARCHIVE_FILE_NAME = f"{SOURCE_FILE_NAME}.gz"

pyspark_job = {
    "main_python_file_uri": "gs://aj-airbnb-project/python-scripts/processAirbnbBookings.py"
}
source_file_path = get_full_gcs_path(GCS_BUCKET_NAME, SOURCE_DIR_PATH, SOURCE_FILE_NAME)
archive_file_path = get_full_gcs_path(
    GCS_BUCKET_NAME, ARCHIVE_DIR_PATH, ARCHIVE_FILE_NAME
)

# Define cluster config
CLUSTER_NAME = "airbnb-processing-cluster"
PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region")
CLUSTER_CONFIG = Variable.get("cluster_config", deserialize_json=True)

# Configure DAG details
# DEFAULT_ARGS = Variable.get("dag_default_args", deserialize_json=True)
DEFAULT_ARGS = {
    "owner": "aj-airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="process_airbnb_data",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    start_date=days_ago(0),
    tags=["airbnb", "dataproc", "bigquery"],
) as dag:

    # Check for daily data file in GCS bucket.
    # checks every min for the file, until next 20 mins.
    gcs_sensor = GCSObjectExistenceSensor(
        task_id="check_bookings_data_arrival",
        bucket=GCS_BUCKET_NAME,
        object=f"landing-zone/incoming-bookings-data/bookings-{datetime.now().strftime('%Y-%m-%d')}.csv",
        timeout=20 * 60,
        poke_interval=60,
        mode="reschedule",
    )

    # Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        cluster_config=CLUSTER_CONFIG,
    )

    # Submit Spark job to the Dataproc cluster
    submit_job = DataprocSubmitPySparkJobOperator(
        task_id="process_data",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        main=pyspark_job["main_python_file_uri"],
        arguments=[source_file_path],
    )

    # Delete Dataproc cluster after processing
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="none_failed",
    )

    # create an archived version of the processed source file
    archive_source_file = BashOperator(
        task_id="archive_source_file",
        bash_command=f"gsutil cp {source_file_path} - | gzip | gsutil cp - {archive_file_path}",
        trigger_rule="all_success",
    )

    # Delete file from source
    delete_source_file = GCSDeleteObjectsOperator(
        task_id="delete_file_from_gcs",
        bucket_name=GCS_BUCKET_NAME,
        objects=f"{SOURCE_DIR_PATH}/{SOURCE_FILE_NAME}",
        trigger_rule="all_success",
    )

    # Define task dependencies
    (
        gcs_sensor
        >> create_cluster
        >> submit_job
        >> delete_cluster
        >> archive_source_file
        >> delete_source_file
    )
