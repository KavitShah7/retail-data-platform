"""Shared pieces for the Kafka -> Spark streaming jobs.

Environment overrides:
    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    KAFKA_TOPIC               default retail_orders
    KAFKA_STARTING_OFFSETS    default earliest (only used on a fresh checkpoint)
"""
import logging
import os

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

log = logging.getLogger("retail_streaming")

# Keep the Kafka connector on the same version as the installed PySpark.
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

# order_timestamp stays a string so existing Parquet/Delta outputs keep their schema.
ORDER_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_timestamp", StringType()),
    StructField("status", StringType()),
    StructField("order_total", DoubleType()),
])


def setup_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def spark_builder(app_name):
    return SparkSession.builder.appName(app_name).master(os.environ.get("SPARK_MASTER", "local[*]"))


def delta_builder(app_name):
    return (
        spark_builder(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )


def read_order_stream(spark):
    """Kafka topic -> typed order rows. Messages that don't parse are dropped."""
    servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "retail_orders")
    log.info("Reading topic %s from %s", topic, servers)

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe", topic)
        .option("startingOffsets", os.environ.get("KAFKA_STARTING_OFFSETS", "earliest"))
        .option("failOnDataLoss", "true")
        .load()
    )
    return (
        raw
        .select(from_json(col("value").cast("string"), ORDER_SCHEMA).alias("order"))
        .select("order.*")
        # from_json yields nulls for malformed JSON; don't store those as orders.
        .where(col("order_id").isNotNull())
    )
