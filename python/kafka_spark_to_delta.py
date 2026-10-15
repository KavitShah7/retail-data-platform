from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)

from delta import configure_spark_with_delta_pip


# --------------------------------------------------
# 1. Create Spark + Delta session
# --------------------------------------------------

builder = (
    SparkSession.builder
    .appName("KafkaRetailOrdersToDelta")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    ]
).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Define incoming Kafka JSON structure
# --------------------------------------------------

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_timestamp", StringType()),
    StructField("status", StringType()),
    StructField("order_total", DoubleType())
])


# --------------------------------------------------
# 3. Read continuously from Kafka
# --------------------------------------------------

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail_orders")
    .option("startingOffsets", "earliest")
    .load()
)


# --------------------------------------------------
# 4. Convert Kafka bytes → JSON → columns
# --------------------------------------------------

orders_df = (
    kafka_df
    .select(
        from_json(
            col("value").cast("string"),
            order_schema
        ).alias("order")
    )
    .select("order.*")
)


# --------------------------------------------------
# 5. Write continuously to Delta Lake
# --------------------------------------------------

query = (
    orders_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("path", "delta/streaming_orders")
    .option(
        "checkpointLocation",
        "delta/checkpoints/streaming_orders"
    )
    .start()
)


# --------------------------------------------------
# 6. Keep streaming job alive
# --------------------------------------------------

query.awaitTermination()