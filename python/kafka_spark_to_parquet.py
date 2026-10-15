from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)


# Create Spark session
spark = (
    SparkSession.builder
    .appName("KafkaRetailOrdersToParquet")
    .getOrCreate()
)

# Reduce noisy Spark logs
spark.sparkContext.setLogLevel("WARN")


# Define the schema of incoming Kafka JSON messages
order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_timestamp", StringType()),
    StructField("status", StringType()),
    StructField("order_total", DoubleType())
])


# Read streaming data from Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail_orders")
    .option("startingOffsets", "earliest")
    .load()
)


# Kafka stores message value as bytes.
# Convert bytes -> string -> JSON -> structured columns.
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


# Write streaming data to Parquet
query = (
    orders_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "streaming/orders")
    .option("checkpointLocation", "streaming/checkpoints/orders")
    .start()
)


# Keep the streaming job running
query.awaitTermination()