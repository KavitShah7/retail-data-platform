from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType
)


spark = (
    SparkSession.builder
    .appName("KafkaRetailOrdersStream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

order_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_timestamp", StringType()),
    StructField("status", StringType()),
    StructField("order_total", DoubleType())
])


kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail_orders")
    .option("startingOffsets", "earliest")
    .load()
)


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


query = (
    orders_df.writeStream
    .format("console")
    .outputMode("append")
    .start()
)


query.awaitTermination()