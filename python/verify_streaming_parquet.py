"""Print what the Parquet streaming job has written so far."""
import os

from streaming_common import spark_builder


def main():
    path = os.environ.get("RETAIL_STREAM_OUTPUT", "streaming/orders")
    spark = spark_builder("VerifyStreamingOrders").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        orders_df = spark.read.parquet(path)
        print("\nSCHEMA")
        orders_df.printSchema()
        print("\nSAMPLE ORDERS")
        orders_df.show(20, truncate=False)
        print(f"\nTOTAL ROW COUNT: {orders_df.count()}")
        print("\nORDER STATUS COUNTS")
        orders_df.groupBy("status").count().show()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
