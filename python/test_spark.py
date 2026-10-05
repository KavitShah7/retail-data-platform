from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("RetailDataPlatform")
    .master("local[*]")
    .getOrCreate()
)

print("Spark started successfully!")
print(f"Spark version: {spark.version}")

spark.stop()