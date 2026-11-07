"""Shared connection to the PostgreSQL database.

Connection settings come from libpq's standard variables (PGHOST, PGPORT,
PGUSER, PGPASSWORD, ...). RETAIL_PG_DBNAME overrides the database name.
"""
import os

import psycopg


def get_connection():
    """Connect to Postgres; fail after 10 seconds instead of hanging."""
    return psycopg.connect(
        dbname=os.environ.get("RETAIL_PG_DBNAME", "retail_db"),
        connect_timeout=int(os.environ.get("RETAIL_PG_CONNECT_TIMEOUT", "10")),
        application_name="retail-data-platform",
    )


if __name__ == "__main__":
    # Quick connectivity check.
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM customers;")
        print(f"Number of customers in the database: {cursor.fetchone()[0]}")
