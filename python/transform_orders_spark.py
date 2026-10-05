from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


spark = (
    SparkSession.builder
    .appName("TransformOrders")
    .master("local[*]")
    .getOrCreate()
)


# Read orders
orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)


# Read order items
order_items_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/order_items.csv")
)


# Calculate total value for each order
order_totals_df = (
    order_items_df
    .groupBy("order_id")
    .agg(
        sum(
            col("quantity") * col("unit_price")
        ).alias("order_total")
    )
)


# Join the calculated totals back to the orders
enriched_orders_df = (
    orders_df
    .join(
        order_totals_df,
        on="order_id",
        how="left"
    )
)


enriched_orders_df.show(10)

enriched_orders_df.printSchema()

print(
    f"Number of orders: {enriched_orders_df.count()}"
)

enriched_orders_df.write \
    .mode("overwrite") \
    .parquet("processed/orders")

spark.stop()