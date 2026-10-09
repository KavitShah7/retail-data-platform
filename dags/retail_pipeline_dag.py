import json
import os
import subprocess
from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


# --------------------------------------------------
# PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(
    "/Users/kavitshah/Documents/Python/retail-data-platform"
)

PROJECT_PYTHON = PROJECT_ROOT / ".venv/bin/python"

RETAIL_BUCKET = "retail-data-platform-kavit-2026"


# --------------------------------------------------
# AIRFLOW DEFAULT SETTINGS
# --------------------------------------------------

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def run_python_script(script_name):
    subprocess.run(
        [
            str(PROJECT_PYTHON),
            str(PROJECT_ROOT / "python" / script_name),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def get_s3_objects(prefix):
    result = subprocess.run(
        [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            RETAIL_BUCKET,
            "--prefix",
            prefix,
            "--output",
            "json",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    response = json.loads(result.stdout)

    return response.get("Contents", [])


# --------------------------------------------------
# DAG
# --------------------------------------------------

@dag(
    dag_id="retail_data_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="UTC",
    ),
    catchup=False,
    default_args=default_args,
    tags=["retail-data-platform"],
)
def retail_pipeline():

    # --------------------------------------------------
    # TASK 1: GENERATE DATA
    # --------------------------------------------------

    @task
    def generate_data():

        scripts = [
            "generate_customers.py",
            "generate_products.py",
            "generate_orders.py",
            "generate_order_items.py",
            "generate_payments.py",
        ]

        for script in scripts:
            print(f"Running {script}")
            run_python_script(script)

        print("All raw datasets generated successfully.")

    # --------------------------------------------------
    # TASK 2: UPLOAD RAW DATA TO S3
    # --------------------------------------------------

    @task
    def upload_raw_to_s3():

        env = os.environ.copy()

        env["RETAIL_BUCKET"] = RETAIL_BUCKET

        subprocess.run(
            [
                str(PROJECT_PYTHON),
                str(
                    PROJECT_ROOT
                    / "python"
                    / "upload_to_s3.py"
                ),
            ],
            cwd=PROJECT_ROOT,
            env=env,
            check=True,
        )

        print("Raw data uploaded to S3 successfully.")

    # --------------------------------------------------
    # TASK 3: RUN SPARK PIPELINE
    # --------------------------------------------------

    @task
    def run_spark_pipeline():

        run_python_script(
            "retail_spark_pipeline.py"
        )

        print("Spark pipeline completed successfully.")

    # --------------------------------------------------
    # TASK 4: UPLOAD PROCESSED PARQUET
    # --------------------------------------------------

    @task
    def upload_processed_to_s3():

        processed_datasets = [
            "orders",
            "customer_summary",
        ]

        for dataset in processed_datasets:

            local_source = (
                PROJECT_ROOT
                / "processed"
                / dataset
            )

            s3_destination = (
                f"s3://{RETAIL_BUCKET}"
                f"/processed/{dataset}/"
            )

            print(
                f"Removing old S3 data from "
                f"{s3_destination}"
            )

            subprocess.run(
                [
                    "aws",
                    "s3",
                    "rm",
                    s3_destination,
                    "--recursive",
                ],
                check=True,
            )

            print(
                f"Uploading new Parquet files "
                f"for {dataset}"
            )

            subprocess.run(
                [
                    "aws",
                    "s3",
                    "cp",
                    str(local_source),
                    s3_destination,
                    "--recursive",
                    "--exclude",
                    "*",
                    "--include",
                    "*.parquet",
                ],
                check=True,
            )

        print(
            "Processed Parquet data uploaded successfully."
        )

    # --------------------------------------------------
    # TASK 5A: VALIDATE RAW DATA
    # --------------------------------------------------

    @task
    def validate_raw_data():

        expected_raw_files = [
            "raw/customers/customers.csv",
            "raw/products/products.csv",
            "raw/orders/orders.csv",
            "raw/order_items/order_items.csv",
            "raw/payments/payments.csv",
        ]

        for key in expected_raw_files:

            print(
                f"Checking "
                f"s3://{RETAIL_BUCKET}/{key}"
            )

            subprocess.run(
                [
                    "aws",
                    "s3api",
                    "head-object",
                    "--bucket",
                    RETAIL_BUCKET,
                    "--key",
                    key,
                ],
                check=True,
                capture_output=True,
                text=True,
            )

        print("Raw data validation passed!")

    # --------------------------------------------------
    # TASK 5B: VALIDATE PROCESSED ORDERS
    # --------------------------------------------------

    @task
    def validate_orders():

        objects = get_s3_objects(
            "processed/orders/"
        )

        parquet_files = [
            obj["Key"]
            for obj in objects
            if obj["Key"].endswith(".parquet")
        ]

        if not parquet_files:
            raise ValueError(
                "No processed orders "
                "Parquet files found."
            )

        print(
            f"Orders validation passed: "
            f"{len(parquet_files)} "
            f"Parquet file(s)"
        )

    # --------------------------------------------------
    # TASK 5C: VALIDATE CUSTOMER SUMMARY
    # --------------------------------------------------

    @task
    def validate_customer_summary():

        objects = get_s3_objects(
            "processed/customer_summary/"
        )

        parquet_files = [
            obj["Key"]
            for obj in objects
            if obj["Key"].endswith(".parquet")
        ]

        if not parquet_files:
            raise ValueError(
                "No customer summary "
                "Parquet files found."
            )

        print(
            f"Customer summary validation passed: "
            f"{len(parquet_files)} "
            f"Parquet file(s)"
        )

    # --------------------------------------------------
    # FINAL TASK
    # --------------------------------------------------

    @task
    def pipeline_complete():

        print(
            "Retail data pipeline "
            "completed successfully!"
        )

    # --------------------------------------------------
    # CREATE TASK INSTANCES
    # --------------------------------------------------

    generate_task = generate_data()

    upload_raw_task = upload_raw_to_s3()

    spark_task = run_spark_pipeline()

    upload_processed_task = upload_processed_to_s3()

    validate_raw_task = validate_raw_data()

    validate_orders_task = validate_orders()

    validate_customer_task = validate_customer_summary()

    complete_task = pipeline_complete()

    # --------------------------------------------------
    # TASK DEPENDENCIES
    # --------------------------------------------------

    (
        generate_task
        >> upload_raw_task
        >> spark_task
        >> upload_processed_task
    )

    upload_processed_task >> [
        validate_raw_task,
        validate_orders_task,
        validate_customer_task,
    ]

    [
        validate_raw_task,
        validate_orders_task,
        validate_customer_task,
    ] >> complete_task


retail_pipeline()