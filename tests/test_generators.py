import csv
import importlib.util
import os
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("faker") is None, reason="Faker not installed"
)

PYTHON_DIR = Path(__file__).resolve().parents[1] / "python"
GENERATORS = [
    "generate_customers.py",
    "generate_products.py",
    "generate_orders.py",
    "generate_order_items.py",
    "generate_payments.py",
]


def run_generators(folder, seed=None, scripts=GENERATORS):
    env = {**os.environ}
    env.pop("RETAIL_SEED", None)
    if seed is not None:
        env["RETAIL_SEED"] = str(seed)
    for script in scripts:
        subprocess.run(
            [sys.executable, str(PYTHON_DIR / script)],
            cwd=folder, env=env, check=True, capture_output=True,
        )


def read(folder, name):
    with (folder / "data" / f"{name}.csv").open(encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_customer_generator_from_empty_directory(tmp_path):
    run_generators(tmp_path, scripts=["generate_customers.py"])
    rows = read(tmp_path, "customers")
    assert len(rows) == 100
    assert {int(row["customer_id"]) for row in rows} == set(range(1, 101))
    assert all(row["email"] and row["state"] for row in rows)


def test_generated_datasets_fit_together(tmp_path):
    run_generators(tmp_path)
    customers = {r["customer_id"] for r in read(tmp_path, "customers")}
    products = {r["product_id"]: float(r["price"]) for r in read(tmp_path, "products")}
    orders = read(tmp_path, "orders")
    items = read(tmp_path, "order_items")
    payments = read(tmp_path, "payments")

    assert len(products) == 50
    assert len(orders) == 1000
    assert {o["customer_id"] for o in orders} <= customers
    assert {i["order_id"] for i in items} == {o["order_id"] for o in orders}
    assert all(float(i["unit_price"]) == products[i["product_id"]] for i in items)

    totals = {}
    for i in items:
        totals[i["order_id"]] = totals.get(i["order_id"], 0) + int(i["quantity"]) * float(i["unit_price"])
    assert len(payments) == len(orders)
    assert all(float(p["payment_amount"]) == round(totals[p["order_id"]], 2) for p in payments)


def test_same_seed_gives_same_data(tmp_path):
    first, second = tmp_path / "a", tmp_path / "b"
    first.mkdir()
    second.mkdir()
    run_generators(first, seed=42)
    run_generators(second, seed=42)
    for name in ["customers", "products", "orders", "order_items", "payments"]:
        assert read(first, name) == read(second, name), name
