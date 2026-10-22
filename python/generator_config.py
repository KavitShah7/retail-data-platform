"""Shared settings for the synthetic data generators.

Set RETAIL_SEED to any integer to get the same data on every run.
"""
import csv
import os
import random
from pathlib import Path

DATA_DIR = Path("data")

NUM_CUSTOMERS = 100
NUM_PRODUCTS = 50
NUM_ORDERS = 1000

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]


def seed_random(faker=None):
    """Seed random (and Faker, if given) from RETAIL_SEED; do nothing if unset."""
    seed = os.environ.get("RETAIL_SEED")
    if seed is None:
        return
    random.seed(int(seed))
    if faker is not None:
        faker.seed_instance(int(seed))


def write_csv(filename, fieldnames, rows):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(DATA_DIR / filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_csv(filename):
    with open(DATA_DIR / filename, newline="", encoding="utf-8") as file:
        return list(csv.DictReader(file))
