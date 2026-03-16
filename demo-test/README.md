# Local Data Lake Demo

Demo pipeline: Kafka -> Spark Structured Streaming -> Delta Lake, orchestrated by Airflow.

## Repository Structure

- `docker-compose.yml` - container definitions: `airflow-web`, `airflow-scheduler`, `zookeeper`, `kafka`, `spark`.
- `Dockerfile.airflow` - Airflow image build.
- `Dockerfile.spark` - Spark image build (Delta + Kafka support expected).
- `producer.py` - Kafka JSON producer.
- `spark_kafka_to_delta.py` - Spark structured-streaming job from Kafka to Delta.
- `read_delta.py` - Spark Delta reader.
- `dags/kafka_to_delta_dag.py` - Airflow DAG for end-to-end orchestration.

## Preconditions

- Docker and Docker Compose installed.
- On Windows, use WSL2 or Docker Desktop.
- Optional: Python3 on host with `kafka-python`, `pyspark`, `delta-spark` to run locally outside containers.

## Quick start (Docker Compose)

1. Open terminal in project folder `c:\app\de\local-data-lake-demo\demo-test`.
2. Build images (first run or after Dockerfile changes):

```bash
docker compose build airflow spark
```

3. Start core services:

```bash
docker compose up -d zookeeper kafka spark
```

3. Wait for Kafka to be healthy (`localhost:9092` exposed).

4. Produce sample messages:

```bash
docker compose exec -T kafka python3 /opt/project/producer.py
```

5. Run Spark streaming job:

```bash
docker compose exec -T spark spark-submit --master local[*] /opt/project/spark_kafka_to_delta.py
```

`docker compose exec -T` is used by DAG; manual host command also works if network path available.

6. Read Delta output:

```bash
docker compose exec -T spark spark-submit --packages io.delta:delta-core_2.12:2.2.0 /opt/project/read_delta.py
```

7. Shutdown services:

```bash
docker compose down
```

## Airflow DAG execution

1. Ensure Airflow service commands are correct in `docker-compose.yml` (no nested bash):

- `airflow-web` command should be plain string:
  `airflow db upgrade || true && airflow users create ... && airflow webserver`
- `airflow-scheduler` command should be plain string:
  `airflow scheduler`

2. Rebuild/pull and deploy:

```bash
docker compose build airflow spark

docker compose up -d airflow-web airflow-scheduler
```

2. Open Airflow UI at `http://localhost:8080`.
   - user: `admin`, password: `admin` (created in compose entrypoint)

3. In Airflow, enable and trigger DAG `kafka_to_delta_demo`.

4. The DAG steps run:
   - `start_kafka` (starts zookeeper, kafka, spark container)
   - `wait_for_kafka`
   - `produce_messages`
   - `run_spark_job`
   - `read_delta`
   - `stop_kafka`

5. Inspect logs in Airflow UI.

## Optional manual host run (no Airflow)

1. Start composed dependencies:

```bash
docker compose up -d zookeeper kafka spark
```

2. Run producer from host if host can access Kafka:

```bash
KAFKA_BOOTSTRAP=127.0.0.1:9092 python producer.py
```

3. Run spark step (requires Spark+Delta libraries in environment):

```bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,io.delta:delta-core_2.2.0 \
  spark_kafka_to_delta.py
```

4. Query table:

```bash
spark-submit --packages io.delta:delta-core_2.12:2.2.0 read_delta.py
```

## Environment variables

- `KAFKA_BOOTSTRAP` (default `kafka:9092` inside containers)
- `KAFKA_TOPIC` (default `input_topic`)
- `DELTA_PATH` (default `/opt/project/data/delta_table`)
- `CHECKPOINT` (default `/opt/project/data/checkpoint`)

## Troubleshooting

- If Kafka is unavailable, verify `docker compose ps` and ports.
- If Spark job fails due missing packages, ensure Docker image has `delta-spark` and Kafka connector, or pass `--packages` as above.
- If Airflow failure on `docker compose` in DAG, ensure `dags` folder mount permissions are correct and path in operator matches `/opt/airflow/project`.

## Notes

- `spark_kafka_to_delta.py` has two embedded definitions; main behavior uses first block and writes to Delta path for streaming.
- `read_delta.py` prints full Delta contents with `df.show()`.
