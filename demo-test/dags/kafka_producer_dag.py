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
    dag_id="kafka_producer",
    default_args=DEFAULT_ARGS,
    description="Produce sample messages into Kafka topic",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    produce_messages = BashOperator(
        task_id="produce_messages",
        bash_command="python /opt/airflow/project/producer.py",
        cwd=PROJECT_ROOT,
    )