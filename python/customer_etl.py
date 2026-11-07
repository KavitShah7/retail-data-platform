"""Postgres ETL: customers -> customer_summary (full name + state), upserted."""
import logging

from db_connection import get_connection

log = logging.getLogger("customer_etl")

SELECT_SQL = "SELECT customer_id, first_name, last_name, state FROM customers"

UPSERT_SQL = """
    INSERT INTO customer_summary (customer_id, full_name, state)
    VALUES (%s, %s, %s)
    ON CONFLICT (customer_id) DO UPDATE SET
        full_name = EXCLUDED.full_name,
        state = EXCLUDED.state
"""


def transform(rows):
    """(id, first, last, state) -> (id, full_name, state); tolerates missing name parts."""
    return [
        (customer_id, " ".join(part for part in (first, last) if part), state)
        for customer_id, first, last, state in rows
    ]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    # `with connection` commits on success and rolls back on any exception.
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(SELECT_SQL)
        summaries = transform(cursor.fetchall())
        cursor.executemany(UPSERT_SQL, summaries)
    log.info("Loaded %d customers.", len(summaries))


if __name__ == "__main__":
    main()
