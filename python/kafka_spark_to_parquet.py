"""Stream Kafka order events into Parquet files.

Environment overrides:
    RETAIL_STREAM_OUTPUT       default streaming/orders
    RETAIL_STREAM_CHECKPOINT   default streaming/checkpoints/orders
"""
import os

from streaming_common import KAFKA_PACKAGE, read_order_stream, setup_logging, spark_builder


def main():
    setup_logging()
    spark = (
        spark_builder("KafkaRetailOrdersToParquet")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = (
        read_order_stream(spark)
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", os.environ.get("RETAIL_STREAM_OUTPUT", "streaming/orders"))
        # The checkpoint records Kafka offsets, so a restart resumes without duplicates.
        .option("checkpointLocation", os.environ.get("RETAIL_STREAM_CHECKPOINT", "streaming/checkpoints/orders"))
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
