from datetime import datetime, timedelta
import socket
import time

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = "/opt/airflow/project"
DOCKER_COMPOSE_FILE = PROJECT_ROOT + "/docker-compose.yml"
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


# DAG 1: Produce messages to Kafka
with DAG(
    dag_id="mkafka_producer",
    default_args=DEFAULT_ARGS,
    description="Produce messages into Kafka topic",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag_kafka_producer:
    produce_messages = BashOperator(
        task_id="produce_messages",
        bash_command=(
            f"docker compose -f \"{DOCKER_COMPOSE_FILE}\" exec -T kafka "
            "python3 /opt/project/producer.py"
        ),
        cwd=PROJECT_ROOT,
    )


# DAG 2: Check Kafka broker active
with DAG(
    dag_id="mkafka_healthcheck",
    default_args=DEFAULT_ARGS,
    description="Check Kafka broker availability",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag_kafka_check:
    healthcheck = PythonOperator(
        task_id="check_kafka_broker",
        python_callable=wait_for_kafka_broker,
    )


# DAG 3: Stream from Kafka to Delta Lake
with DAG(
    dag_id="mkafka_stream_to_delta",
    default_args=DEFAULT_ARGS,
    description="Stream Kafka data into Delta Lake",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag_kafka_stream:
    stream_job = BashOperator(
        task_id="run_spark_stream",
        bash_command=(
            f"docker compose -f \"{DOCKER_COMPOSE_FILE}\" exec -T spark "
            "spark-submit --master local[*] "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-core_2.12:2.2.0 "
            f"{PROJECT_ROOT}/spark_kafka_to_delta.py"
        ),
        cwd=PROJECT_ROOT,
    )


# DAG 4: Read data from Delta Lake
with DAG(
    dag_id="mdelta_read",
    default_args=DEFAULT_ARGS,
    description="Read data from Delta Lake",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag_delta_read:
    read_delta = BashOperator(
        task_id="read_delta",
        bash_command=(
            f"docker compose -f \"{DOCKER_COMPOSE_FILE}\" exec -T spark "
            "spark-submit --packages io.delta:delta-core_2.12:2.2.0 "
            f"{PROJECT_ROOT}/read_delta.py"
        ),
        cwd=PROJECT_ROOT,
    )
