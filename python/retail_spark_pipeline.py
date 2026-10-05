from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct


spark = (
    SparkSession.builder
    .appName("RetailDataPipeline")
    .master("local[*]")
    .getOrCreate()
)


# -------------------------
# EXTRACT
# -------------------------

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/customers.csv")
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/orders.csv")
)

order_items_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/order_items.csv")
)

payments_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("data/payments.csv")
)


# -------------------------
# TRANSFORM: ORDER TOTALS
# -------------------------

order_totals_df = (
    order_items_df
    .groupBy("order_id")
    .agg(
        sum(
            col("quantity") * col("unit_price")
        ).alias("order_total")
    )
)


# -------------------------
# TRANSFORM: ENRICH ORDERS
# -------------------------

processed_orders_df = (
    orders_df
    .join(
        order_totals_df,
        on="order_id",
        how="left"
    )
    .join(
        payments_df.select(
            "order_id",
            "payment_amount",
            "payment_method"
        ),
        on="order_id",
        how="left"
    )
)


# -------------------------
# TRANSFORM: CUSTOMER METRICS
# -------------------------

customer_metrics_df = (
    processed_orders_df
    .groupBy("customer_id")
    .agg(
        countDistinct("order_id").alias("order_count"),
        sum("order_total").alias("total_revenue")
    )
)


customer_summary_df = (
    customers_df
    .join(
        customer_metrics_df,
        on="customer_id",
        how="left"
    )
)


# -------------------------
# LOAD
# -------------------------

processed_orders_df.write \
    .mode("overwrite") \
    .parquet("processed/orders")


customer_summary_df.write \
    .mode("overwrite") \
    .parquet("processed/customer_summary")


print("Retail Spark pipeline completed successfully!")

print("Processed orders:")
processed_orders_df.show(5)

print("Customer summary:")
customer_summary_df.show(5)


spark.stop()