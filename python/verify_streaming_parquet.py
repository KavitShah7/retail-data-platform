from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("VerifyStreamingOrders")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


orders_df = spark.read.parquet("streaming/orders")


print("\nSCHEMA")
orders_df.printSchema()


print("\nSAMPLE ORDERS")
orders_df.show(20, truncate=False)


print("\nTOTAL ROW COUNT")
print(orders_df.count())


print("\nORDER STATUS COUNTS")
orders_df.groupBy("status").count().show()


spark.stop()