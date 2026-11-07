"""Stream Kafka order events into a Delta Lake table.

Environment overrides:
    RETAIL_DELTA_PATH        default delta/streaming_orders
    RETAIL_DELTA_CHECKPOINT  default delta/checkpoints/streaming_orders
"""
import os

from delta import configure_spark_with_delta_pip

from streaming_common import KAFKA_PACKAGE, delta_builder, read_order_stream, setup_logging


def main():
    setup_logging()
    spark = configure_spark_with_delta_pip(
        delta_builder("KafkaRetailOrdersToDelta"),
        extra_packages=[KAFKA_PACKAGE],
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    query = (
        read_order_stream(spark)
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("path", os.environ.get("RETAIL_DELTA_PATH", "delta/streaming_orders"))
        # The checkpoint records Kafka offsets, so a restart resumes without duplicates.
        .option(
            "checkpointLocation",
            os.environ.get("RETAIL_DELTA_CHECKPOINT", "delta/checkpoints/streaming_orders"),
        )
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
