"""Print a sample of customers from Postgres (read-only)."""
import sys

from db_connection import get_connection


def main(limit=10):
    with get_connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT customer_id, first_name, last_name, state FROM customers ORDER BY customer_id LIMIT %s",
            (limit,),
        )
        for customer_id, first_name, last_name, state in cursor.fetchall():
            print(customer_id, f"{first_name} {last_name}", state)


if __name__ == "__main__":
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 10)
