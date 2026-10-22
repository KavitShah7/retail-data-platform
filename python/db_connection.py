"""Shared connection to the local PostgreSQL database."""
import os

import psycopg


def get_connection():
    """Connect to the local Postgres database (override name with RETAIL_PG_DBNAME)."""
    return psycopg.connect(dbname=os.environ.get("RETAIL_PG_DBNAME", "retail_db"))


if __name__ == "__main__":
    # Quick connectivity check.
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM customers;")
        print(f"Number of customers in the database: {cursor.fetchone()[0]}")
