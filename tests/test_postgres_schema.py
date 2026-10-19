"""Check the declared SQL tables with SQLite; this is not a PostgreSQL integration test."""
import sqlite3
from pathlib import Path


def test_customer_etl_destination_is_declared():
    schema = Path(__file__).resolve().parents[1] / "sql/01_create_tables.sql"
    with sqlite3.connect(":memory:") as connection:
        connection.executescript(schema.read_text())
        connection.execute("INSERT INTO customers (customer_id) VALUES (1)")
        connection.execute(
            "INSERT INTO customer_summary (customer_id, full_name, state) VALUES (?, ?, ?)",
            (1, "Test Customer", "NY"),
        )
        assert connection.execute("SELECT full_name FROM customer_summary").fetchone() == ("Test Customer",)
