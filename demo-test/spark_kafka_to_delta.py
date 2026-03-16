import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, LongType, StringType


DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "/opt/project/data/delta_table")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/project/data/checkpoint")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "demo-topic")


def build_spark():
    spark = (
        SparkSession.builder
        .appName("kafka_to_delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    return spark


def main():
    spark = build_spark()

    schema = StructType([
        StructField("id", LongType(), True),
        StructField("message", StringType(), True),
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    json_df = df.selectExpr("CAST(value AS STRING) AS json_str")

    parsed = (
        json_df
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withColumn("processed_at", expr("current_timestamp()"))
    )

    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start(DELTA_TABLE_PATH)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()