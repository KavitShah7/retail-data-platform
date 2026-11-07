"""Debug stream: print Kafka order events to the console."""
from streaming_common import KAFKA_PACKAGE, read_order_stream, setup_logging, spark_builder


def main():
    setup_logging()
    spark = (
        spark_builder("KafkaRetailOrdersStream")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    query = (
        read_order_stream(spark)
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
