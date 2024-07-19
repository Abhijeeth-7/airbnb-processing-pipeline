from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago


def create_task_group(
    task_group_id,
    bucket_name,
    file_name,
    cluster_name,
    region,
    cluster_config,
    pyspark_job,
    source_file_path,
    archive_file_path,
):
    with TaskGroup(task_group_id) as Airbnb_pipeline:
        check_for_file = GCSObjectExistenceSensor(
            task_id="check_for_file",
            bucket=bucket_name,
            object=file_name,
            timeout=20 * 60,
            poke_interval=60,
            mode="reschedule",
        )

        create_cluster = DataprocCreateClusterOperator(
            task_id="create_cluster",
            cluster_name=cluster_name,
            region=region,
            cluster_config=cluster_config,
        )

        submit_pyspark_job = DataprocSubmitPySparkJobOperator(
            task_id="submit_pyspark_job",
            cluster_name=cluster_name,
            region=region,
            main=pyspark_job["main_python_file_uri"],
            arguments=[source_file_path, ],
        )

        delete_cluster = DataprocDeleteClusterOperator(
            task_id="delete_cluster",
            cluster_name=cluster_name,
            region=region,
            trigger_rule="none_failed",
        )

        archive_file = BashOperator(
            task_id="archive_file",
            bash_command=f"gsutil cp {source_file_path} - | gzip | gsutil cp - {archive_file_path}",
            trigger_rule="none_failed",
        )

        delete_source_file = GCSDeleteObjectsOperator(
            task_id="delete_source_file",
            bucket_name=bucket_name,
            objects=[file_name],
            trigger_rule="none_failed",
        )

        (
            check_for_file
            >> create_cluster
            >> submit_pyspark_job
            >> delete_cluster
            >> archive_file
            >> delete_source_file
        )

    return Airbnb_pipeline
