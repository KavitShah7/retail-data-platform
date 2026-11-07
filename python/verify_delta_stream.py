"""Print what the Delta streaming job has written so far."""
import os

from delta import configure_spark_with_delta_pip

from streaming_common import delta_builder


def main():
    path = os.environ.get("RETAIL_DELTA_PATH", "delta/streaming_orders")
    spark = configure_spark_with_delta_pip(delta_builder("VerifyDeltaStream")).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        df = spark.read.format("delta").load(path)
        print("\nDELTA STREAMING DATA")
        df.show(20, truncate=False)
        print(f"\nTOTAL ROW COUNT: {df.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
