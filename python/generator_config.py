"""Shared settings for the synthetic data generators.

Environment overrides:
    RETAIL_SEED       any integer; gives the same data on every run
    RETAIL_DATA_DIR   output folder for CSVs (default: ./data)
    RETAIL_NUM_CUSTOMERS / RETAIL_NUM_PRODUCTS / RETAIL_NUM_ORDERS
"""
import csv
import os
import random
import tempfile
from pathlib import Path


def _positive_int_env(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}")
    return value


DATA_DIR = Path(os.environ.get("RETAIL_DATA_DIR", "data"))

NUM_CUSTOMERS = _positive_int_env("RETAIL_NUM_CUSTOMERS", 100)
NUM_PRODUCTS = _positive_int_env("RETAIL_NUM_PRODUCTS", 50)
NUM_ORDERS = _positive_int_env("RETAIL_NUM_ORDERS", 1000)

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]


def seed_random(faker=None):
    """Seed random (and Faker, if given) from RETAIL_SEED; do nothing if unset."""
    seed = os.environ.get("RETAIL_SEED")
    if not seed:
        return
    random.seed(int(seed))
    if faker is not None:
        faker.seed_instance(int(seed))


def write_csv(filename, fieldnames, rows):
    """Write rows atomically: readers never see a half-written file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / filename
    fd, tmp_name = tempfile.mkstemp(dir=DATA_DIR, prefix=f".{filename}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        os.replace(tmp_name, target)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise


def read_csv(filename):
    path = DATA_DIR / filename
    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found; run the upstream generator first."
        )
    with open(path, newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))
