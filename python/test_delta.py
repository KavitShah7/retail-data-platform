from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


builder = (
    SparkSession.builder
    .appName("TestDelta")
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

data = [
    (1, "pending", 100.50),
    (2, "shipped", 250.75),
    (3, "delivered", 80.25)
]

df = spark.createDataFrame(
    data,
    ["order_id", "status", "order_total"]
)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("delta/orders_test")

print("Delta table written successfully")

spark.read \
    .format("delta") \
    .load("delta/orders_test") \
    .show()

spark.stop()