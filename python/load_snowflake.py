"""Load processed Parquet from the S3 stage into Snowflake, replacing both tables atomically."""
import logging
import os
from pathlib import Path

import snowflake.connector

log = logging.getLogger("load_snowflake")


def required_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} environment variable is not set.")
    return value


# 1. Connect to Snowflake using our existing service account
def connect_to_snowflake():
    return snowflake.connector.connect(
        account=required_env("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER", "RETAIL_DBT_USER"),
        role=os.environ.get("SNOWFLAKE_ROLE", "RETAIL_DBT_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "RETAIL_WH"),
        database="RETAIL_DB",
        schema="PROCESSED",
        authenticator="SNOWFLAKE_JWT",
        private_key_file=str(
            Path(os.environ.get(
                "SNOWFLAKE_PRIVATE_KEY_FILE",
                Path.home() / ".dbt/snowflake_dbt_key.p8",
            )).expanduser()
        ),
        private_key_password=required_env("SNOWFLAKE_KEY_PASSPHRASE"),
        login_timeout=60,
        network_timeout=600,
        # Makes this job's queries easy to find in Snowflake query history.
        session_parameters={"QUERY_TAG": "retail_data_platform:load_snowflake"},
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
        raise RuntimeError(f"Loading failed for {table}: {results}")

    log.info("%s: %d rows loaded into temporary table", table, rows_loaded)
    return rows_loaded


# 3. Execute the complete loading pipeline
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    connection = connect_to_snowflake()
    cursor = None
    transaction_started = False

    try:
        cursor = connection.cursor()
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

        log.info("Snowflake loading completed successfully.")

    except Exception:
        if transaction_started:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                # Keep the original error; Snowflake also rolls back on disconnect.
                log.exception("ROLLBACK failed")
        raise

    finally:
        try:
            if cursor is not None:
                cursor.close()
        finally:
            connection.close()


if __name__ == "__main__":
    main()