from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, sum


# -------------------------
# TRANSFORMS
# -------------------------

def build_order_totals(order_items_df):
    """One row per order: order_total = sum of quantity * unit_price."""
    return (
        order_items_df
        .groupBy("order_id")
        .agg(sum(col("quantity") * col("unit_price")).alias("order_total"))
    )


def build_processed_orders(orders_df, order_items_df, payments_df):
    """Orders enriched with their total and payment details."""
    return (
        orders_df
        .join(build_order_totals(order_items_df), on="order_id", how="left")
        .join(
            payments_df.select("order_id", "payment_amount", "payment_method"),
            on="order_id",
            how="left",
        )
    )


def build_customer_summary(customers_df, processed_orders_df):
    """Every customer, with their order count and total revenue."""
    metrics_df = (
        processed_orders_df
        .groupBy("customer_id")
        .agg(
            countDistinct("order_id").alias("order_count"),
            sum("order_total").alias("total_revenue"),
        )
    )
    return customers_df.join(metrics_df, on="customer_id", how="left")


# -------------------------
# EXTRACT / LOAD
# -------------------------

def read_csv(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def main():
    spark = (
        SparkSession.builder
        .appName("RetailDataPipeline")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        processed_orders_df = build_processed_orders(
            read_csv(spark, "data/orders.csv"),
            read_csv(spark, "data/order_items.csv"),
            read_csv(spark, "data/payments.csv"),
        )
        customer_summary_df = build_customer_summary(
            read_csv(spark, "data/customers.csv"),
            processed_orders_df,
        )

        processed_orders_df.write.mode("overwrite").parquet("processed/orders")
        customer_summary_df.write.mode("overwrite").parquet("processed/customer_summary")

        print("Retail Spark pipeline completed successfully!")
        print("Processed orders:")
        processed_orders_df.show(5)
        print("Customer summary:")
        customer_summary_df.show(5)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
