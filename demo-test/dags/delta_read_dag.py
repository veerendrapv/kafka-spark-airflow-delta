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
    dag_id="delta_read",
    default_args=DEFAULT_ARGS,
    description="Read Delta table using Spark",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    read_delta = BashOperator(
        task_id="read_delta",
        bash_command=(
            "docker exec demo_spark "
            "spark-submit "
            "--packages io.delta:delta-core_2.12:2.2.0 "
            "/opt/project/read_delta.py"
        ),
        cwd=PROJECT_ROOT,
    )