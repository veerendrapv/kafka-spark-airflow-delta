from pyspark.sql import SparkSession
import os


def build_spark():
    builder = SparkSession.builder.appName("read-delta")
    builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder = builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    return builder.getOrCreate()


def main():
    delta_path = os.environ.get("DELTA_PATH", "/opt/project/data/delta_table")
    spark = build_spark()
    if not os.path.exists(delta_path):
        print("Delta path does not exist:", delta_path)
        return
    df = spark.read.format("delta").load(delta_path)
    df.show(truncate=False)


if __name__ == "__main__":
    main()
from pyspark.sql import SparkSession
import os

DELTA_TABLE_PATH = os.getenv("DELTA_TABLE_PATH", "/opt/project/data/delta_table")

spark = (
    SparkSession.builder
    .appName("read_delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load(DELTA_TABLE_PATH)
df.show()
spark.stop()
