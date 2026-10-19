import os
from pathlib import Path

import snowflake.connector


# 1. Connect to Snowflake using our existing service account
def connect_to_snowflake():
    return snowflake.connector.connect(
        account="XKGTHBR-CT45017",
        user="RETAIL_DBT_USER",
        role="RETAIL_DBT_ROLE",
        warehouse="RETAIL_WH",
        database="RETAIL_DB",
        schema="PROCESSED",
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(
            Path.home() / ".dbt/snowflake_dbt_key.p8"
        ),
        private_key_password=os.environ[
            "SNOWFLAKE_KEY_PASSPHRASE"
        ],
    )


# 2. Load Parquet into a temporary table
def load_dataset(cursor, table, columns, source_folder):
    cursor.execute(f"""
        CREATE OR REPLACE TEMPORARY TABLE {table}_LOAD_TMP
        LIKE {table}
    """)

    cursor.execute(f"""
        COPY INTO {table}_LOAD_TMP
        FROM (
            SELECT {columns}
            FROM @RETAIL_DB.PROCESSED.RETAIL_PROCESSED_STAGE/{source_folder}/
        )
        FILE_FORMAT = (
            FORMAT_NAME = 'RETAIL_DB.PROCESSED.PARQUET_FORMAT'
        )
        PATTERN = '.*[.]parquet'
        ON_ERROR = ABORT_STATEMENT
    """)

    results = cursor.fetchall()

    rows_loaded = sum(
        int(row[3]) for row in results
        if row[1] == "LOADED"
    )

    if rows_loaded == 0 or any(
        row[1] != "LOADED" for row in results
    ):
        raise RuntimeError(f"Loading failed for {table}")

    print(f"{table}: {rows_loaded} rows loaded into temporary table")


# 3. Execute the complete loading pipeline
def main():
    connection = connect_to_snowflake()
    cursor = connection.cursor()
    transaction_started = False

    try:
        load_dataset(
            cursor,
            "ORDERS",
            """
            $1:order_id::NUMBER,
            $1:customer_id::NUMBER,
            $1:order_date::DATE,
            $1:status::VARCHAR,
            $1:order_total::FLOAT
            """,
            "orders",
        )

        load_dataset(
            cursor,
            "CUSTOMER_SUMMARY",
            """
            $1:customer_id::NUMBER,
            $1:first_name::VARCHAR,
            $1:last_name::VARCHAR,
            $1:email::VARCHAR,
            $1:city::VARCHAR,
            $1:state::VARCHAR,
            $1:order_count::NUMBER,
            $1:total_revenue::FLOAT
            """,
            "customer_summary",
        )

        # Replace both datasets in one transaction
        cursor.execute("BEGIN TRANSACTION")
        transaction_started = True

        cursor.execute("DELETE FROM ORDERS")
        cursor.execute(
            "INSERT INTO ORDERS SELECT * FROM ORDERS_LOAD_TMP"
        )

        cursor.execute("DELETE FROM CUSTOMER_SUMMARY")
        cursor.execute(
            """
            INSERT INTO CUSTOMER_SUMMARY
            SELECT * FROM CUSTOMER_SUMMARY_LOAD_TMP
            """
        )

        cursor.execute("COMMIT")
        transaction_started = False

        print("Snowflake loading completed successfully!")

    except Exception:
        if transaction_started:
            cursor.execute("ROLLBACK")
        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()