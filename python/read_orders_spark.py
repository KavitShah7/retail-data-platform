from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("ReadOrders")
    .master("local[*]")
    .getOrCreate()
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)

orders_df.show(10)

orders_df.printSchema()

spark.stop()