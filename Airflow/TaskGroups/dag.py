from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = Variable.get("default_args", deserialize=True)
CLUSTER_CONFIG = Variable.get("cluster_config", deserialize_json=True)

GCS_BUCKET_NAME = Variable.get("bucket_name")
PROJECT_ID = Variable.get("project_id")
REGION = Variable.get("region")

SOURCE_DIR_PATH = "gs://landing-zone/incoming-data"
ARCHIVE_DIR_PATH = "gs://landing-zone/archive"

BOOKINGS_FILE_NAME = f"bookings-{datetime.now().strftime('%Y-%m-%d')}.csv"
REVIWES_FILE_NAME = f"bookings-{datetime.now().strftime('%Y-%m-%d')}.csv"
PAYMENTS_FILE_NAME = f"bookings-{datetime.now().strftime('%Y-%m-%d')}.csv"


with DAG(
    dag_id="process_airbnb_data",
    default_args=DEFAULT_ARGS,
    description="An example DAG with parallel TaskGroups",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
) as dag:


    # File and job details for each file type
    task_group_config = [
        {
            "task_group_id": "bookings_data_pipeline",
            "cluster_name": "bookings-processing-cluster",
            "pyspark_job": {"main_python_file_uri": "gs://your-bucket/processAirbnbBookings.py"},
            "file_name": BOOKINGS_FILE_NAME,
            "source_file_path": f"{SOURCE_DIR_PATH}/{BOOKINGS_FILE_NAME}",
            "archive_file_path": f"{ARCHIVE_DIR_PATH}/{BOOKINGS_FILE_NAME}.gz",
        },
        {
            "task_group_id": "reviews_data_pipeline",
            "cluster_name": "reviews-processing-cluster",
            "pyspark_job": {"main_python_file_uri": "gs://your-bucket/processAirbnbReviews.py"},
            "file_name": REVIWES_FILE_NAME,
            "source_file_path": f"{SOURCE_DIR_PATH}/{REVIWES_FILE_NAME}",
            "archive_file_path": f"{ARCHIVE_DIR_PATH}/{REVIWES_FILE_NAME}.gz",
        },
        {
            "task_group_id": "payments_data_pipeline",
            "cluster_name": "payments-processing-cluster",
            "pyspark_job": {"main_python_file_uri": "gs://your-bucket/processAirbnbPayments.py"},
            "file_name": PAYMENTS_FILE_NAME,
            "source_file_path": f"{SOURCE_DIR_PATH}/{PAYMENTS_FILE_NAME}",
            "archive_file_path": f"{ARCHIVE_DIR_PATH}/{PAYMENTS_FILE_NAME}.gz",
        },
    ]

    # Create TaskGroups for each file type
    for config in task_group_config:
        proceesing_pipeline = create_task_group(
            task_group_id=config["task_group_id"],
            bucket_name=bucket_name,
            file_name=config["file_name"],
            cluster_name=config["cluster_name"],
            region=region,
            cluster_config=CLUSTER_CONFIG,
            pyspark_job=config["pyspark_job"],
            source_file_path=config["source_file_path"],
            archive_file_path=config["archive_file_path"],
        )
