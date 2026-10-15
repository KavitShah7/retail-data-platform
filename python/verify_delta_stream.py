from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("VerifyDeltaStream")
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

spark = configure_spark_with_delta_pip(builder).getOrCreate()

spark.sparkContext.setLogLevel("WARN")


df = (
    spark.read
    .format("delta")
    .load("delta/streaming_orders")
)


print("\nDELTA STREAMING DATA")
df.show(20, truncate=False)


print("\nTOTAL ROW COUNT")
print(df.count())


spark.stop()