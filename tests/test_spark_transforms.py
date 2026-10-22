"""Run the real Spark transforms on a few hand-made rows (local Spark, no cloud)."""
import os
import sys

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession

from python.retail_spark_pipeline import (
    build_customer_summary,
    build_order_totals,
    build_processed_orders,
)


@pytest.fixture(scope="module")
def spark():
    # Spark workers must use the same Python as the test run.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("retail-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def data(spark):
    customers = spark.createDataFrame(
        [(1, "Ann", "a@x.com"), (2, "Bob", "b@x.com"), (3, "Cy", "c@x.com")],
        ["customer_id", "first_name", "email"],
    )
    orders = spark.createDataFrame(
        [(10, 1, "shipped"), (11, 1, "pending"), (12, 2, "delivered"), (13, 2, "cancelled")],
        ["order_id", "customer_id", "status"],
    )
    order_items = spark.createDataFrame(
        [(10, 2, 5.0), (10, 1, 10.0), (11, 3, 1.5), (12, 1, 100.0)],  # order 13 has no items
        ["order_id", "quantity", "unit_price"],
    )
    payments = spark.createDataFrame(
        [(10, 20.0, "paypal"), (11, 4.5, "debit_card"), (12, 100.0, "credit_card")],
        ["order_id", "payment_amount", "payment_method"],
    )
    return customers, orders, order_items, payments


def rows_by(df, key):
    return {row[key]: row.asDict() for row in df.collect()}


def test_order_totals_sum_quantity_times_price(data):
    _, _, order_items, _ = data
    totals = rows_by(build_order_totals(order_items), "order_id")
    assert totals == {
        10: {"order_id": 10, "order_total": 20.0},
        11: {"order_id": 11, "order_total": 4.5},
        12: {"order_id": 12, "order_total": 100.0},
    }


def test_processed_orders_keep_every_order_once(data):
    _, orders, order_items, payments = data
    result = build_processed_orders(orders, order_items, payments)
    by_id = rows_by(result, "order_id")

    assert result.count() == orders.count()
    assert set(by_id) == {10, 11, 12, 13}
    assert by_id[10]["order_total"] == 20.0
    assert by_id[10]["payment_method"] == "paypal"
    # An order with no items or payment is kept, with empty totals.
    assert by_id[13]["order_total"] is None
    assert by_id[13]["payment_amount"] is None
    # Every order has the keys dbt treats as required.
    assert all(r["order_id"] is not None and r["customer_id"] is not None for r in by_id.values())


def test_customer_summary_counts_orders_and_revenue(data):
    customers, orders, order_items, payments = data
    processed = build_processed_orders(orders, order_items, payments)
    summary = rows_by(build_customer_summary(customers, processed), "customer_id")

    assert set(summary) == {1, 2, 3}
    assert summary[1]["order_count"] == 2
    assert summary[1]["total_revenue"] == 24.5
    assert summary[2]["order_count"] == 2
    assert summary[2]["total_revenue"] == 100.0  # missing totals are ignored
    # A customer with no orders is still listed.
    assert summary[3]["order_count"] is None
    assert summary[3]["total_revenue"] is None
