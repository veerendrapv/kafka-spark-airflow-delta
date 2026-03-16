from datetime import datetime, timedelta
import socket
import time

from airflow import DAG
from airflow.operators.python import PythonOperator

KAFKA_HOST = "kafka"
KAFKA_PORT = 9092

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


def wait_for_kafka_broker(host: str = KAFKA_HOST, port: int = KAFKA_PORT, timeout: int = 60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=3):
                print(f"Kafka is available at {host}:{port}")
                return True
        except Exception as exc:
            print(f"Kafka not ready yet ({exc})")
            time.sleep(1)
    raise TimeoutError(f"Kafka broker not available at {host}:{port} after {timeout}s")


with DAG(
    dag_id="kafka_healthcheck",
    default_args=DEFAULT_ARGS,
    description="Check Kafka broker availability",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    check_kafka_broker = PythonOperator(
        task_id="check_kafka_broker",
        python_callable=wait_for_kafka_broker,
    )
