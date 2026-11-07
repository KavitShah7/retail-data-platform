"""Batch job: raw retail CSVs -> processed orders + customer summary (Parquet).

Environment overrides:
    RETAIL_DATA_DIR       input CSV folder (default: data)
    RETAIL_PROCESSED_DIR  output Parquet folder (default: processed)
"""
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

log = logging.getLogger(__name__)

# -------------------------
# SCHEMAS
# -------------------------
# Explicit schemas: types never drift with the data, and Spark skips the extra
# pass over each file that inferSchema needs.

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("order_date", DateType()),
    StructField("status", StringType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("payment_amount", DoubleType()),
    StructField("payment_method", StringType()),
])


class DataQualityError(ValueError):
    """Raised when processed data breaks an invariant; nothing is written."""


# -------------------------
# TRANSFORMS
# -------------------------

def build_order_totals(order_items_df):
    """One row per order: order_total = sum of quantity * unit_price, to the cent."""
    return (
        order_items_df
        .groupBy("order_id")
        .agg(F.round(F.sum(F.col("quantity") * F.col("unit_price")), 2).alias("order_total"))
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
    """Every customer, with their order count and total revenue (0 if none)."""
    metrics_df = (
        processed_orders_df
        .groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("order_total").alias("total_revenue"),
        )
    )
    return (
        customers_df
        .join(metrics_df, on="customer_id", how="left")
        .withColumn("order_count", F.coalesce("order_count", F.lit(0).cast("long")))
        .withColumn("total_revenue", F.round(F.coalesce("total_revenue", F.lit(0.0)), 2))
    )


# -------------------------
# DATA QUALITY
# -------------------------

def check_quality(orders_df: DataFrame, processed_orders_df: DataFrame, customer_summary_df: DataFrame):
    """Fail before writing if the outputs break basic invariants."""
    source_orders = orders_df.count()
    if source_orders == 0:
        raise DataQualityError("orders input is empty")

    processed = processed_orders_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("order_id").alias("distinct_ids"),
        F.sum(F.col("order_id").isNull().cast("int")).alias("null_order_ids"),
        F.sum(F.col("customer_id").isNull().cast("int")).alias("null_customer_ids"),
    ).first()

    problems = []
    if processed["rows"] != source_orders:
        # Usually a duplicate payment row fanning out the join.
        problems.append(f"processed orders has {processed['rows']} rows, expected {source_orders}")
    if processed["distinct_ids"] != processed["rows"]:
        problems.append("processed orders has duplicate order_id values")
    if processed["null_order_ids"] or processed["null_customer_ids"]:
        problems.append("processed orders has null order_id or customer_id")

    summary = customer_summary_df.agg(
        F.count("*").alias("rows"),
        F.countDistinct("customer_id").alias("distinct_ids"),
    ).first()
    if summary["rows"] != summary["distinct_ids"]:
        problems.append("customer summary has duplicate customer_id values")

    if problems:
        raise DataQualityError("; ".join(problems))
    log.info("Quality checks passed: %d orders, %d customers", source_orders, summary["rows"])


# -------------------------
# EXTRACT / LOAD
# -------------------------

def read_csv(spark, path, schema):
    # FAILFAST: a malformed row stops the job instead of becoming silent nulls.
    return spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(path)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    data_dir = os.environ.get("RETAIL_DATA_DIR", "data")
    processed_dir = os.environ.get("RETAIL_PROCESSED_DIR", "processed")

    spark = (
        SparkSession.builder
        .appName("RetailDataPipeline")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .getOrCreate()
    )

    try:
        orders_df = read_csv(spark, f"{data_dir}/orders.csv", ORDERS_SCHEMA)
        processed_orders_df = build_processed_orders(
            orders_df,
            read_csv(spark, f"{data_dir}/order_items.csv", ORDER_ITEMS_SCHEMA),
            read_csv(spark, f"{data_dir}/payments.csv", PAYMENTS_SCHEMA),
        ).cache()
        customer_summary_df = build_customer_summary(
            read_csv(spark, f"{data_dir}/customers.csv", CUSTOMERS_SCHEMA),
            processed_orders_df,
        ).cache()

        check_quality(orders_df, processed_orders_df, customer_summary_df)

        processed_orders_df.write.mode("overwrite").parquet(f"{processed_dir}/orders")
        customer_summary_df.write.mode("overwrite").parquet(f"{processed_dir}/customer_summary")
        log.info("Wrote %s/orders and %s/customer_summary", processed_dir, processed_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
