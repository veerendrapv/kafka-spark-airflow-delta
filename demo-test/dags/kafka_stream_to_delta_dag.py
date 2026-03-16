from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/project"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="kafka_stream_to_delta",
    default_args=DEFAULT_ARGS,
    description="Stream data from Kafka to Delta Lake",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    stream_job = BashOperator(
        task_id="run_spark_stream",
        bash_command=(
            "docker exec demo_spark "
            "spark-submit --master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-core_2.12:2.2.0 "
            "/opt/project/spark_kafka_to_delta.py"
        ),
        cwd=PROJECT_ROOT,
    )