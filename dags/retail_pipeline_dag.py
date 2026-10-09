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

# Use the main project environment for Spark/boto3 scripts,
# not the Airflow virtual environment.
PROJECT_PYTHON = PROJECT_ROOT / ".venv/bin/python"

RETAIL_BUCKET = "retail-data-platform-kavit-2026"


# --------------------------------------------------
# AIRFLOW DEFAULT TASK SETTINGS
# --------------------------------------------------

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# --------------------------------------------------
# HELPER FUNCTIONS
# --------------------------------------------------

def run_python_script(script_name):
    """
    Run one of our existing project Python scripts
    using the project's .venv.
    """

    subprocess.run(
        [
            str(PROJECT_PYTHON),
            str(PROJECT_ROOT / "python" / script_name),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def get_s3_objects(prefix):
    """
    Return the S3 objects underneath a prefix.
    """

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
# DAG DEFINITION
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

    # ----------------------------------------------
    # TASK 1: GENERATE RAW DATA
    # ----------------------------------------------

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

    # ----------------------------------------------
    # TASK 2: UPLOAD RAW CSV FILES TO S3
    # ----------------------------------------------

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

    # ----------------------------------------------
    # TASK 3: RUN PYSPARK TRANSFORMATIONS
    # ----------------------------------------------

    @task
    def run_spark_pipeline():

        run_python_script(
            "retail_spark_pipeline.py"
        )

        print("Spark transformation completed.")

    # ----------------------------------------------
    # TASK 4: UPLOAD PROCESSED PARQUET TO S3
    # ----------------------------------------------

    @task
    def upload_processed_to_s3():

        processed_datasets = [
            "orders",
            "customer_summary",
        ]

        for dataset in processed_datasets:

            s3_destination = (
                f"s3://{RETAIL_BUCKET}"
                f"/processed/{dataset}/"
            )

            local_source = (
                PROJECT_ROOT
                / "processed"
                / dataset
            )

            print(
                f"Removing previous S3 data: "
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
                f"Uploading new Parquet data "
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
            "Processed Parquet data uploaded "
            "successfully."
        )

    # ----------------------------------------------
    # TASK 5: VALIDATE PIPELINE OUTPUT
    # ----------------------------------------------

    @task
    def validate_pipeline():

        print("Starting pipeline validation...")

        # Expected raw S3 files
        expected_raw_files = [
            "raw/customers/customers.csv",
            "raw/products/products.csv",
            "raw/orders/orders.csv",
            "raw/order_items/order_items.csv",
            "raw/payments/payments.csv",
        ]

        # Check every raw file exists.
        for key in expected_raw_files:

            print(f"Checking s3://{RETAIL_BUCKET}/{key}")

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

        print("All raw files exist.")

        # Validate processed orders
        order_objects = get_s3_objects(
            "processed/orders/"
        )

        order_parquet_files = [
            obj["Key"]
            for obj in order_objects
            if obj["Key"].endswith(".parquet")
        ]

        if not order_parquet_files:
            raise ValueError(
                "Validation failed: "
                "No processed orders Parquet "
                "files found."
            )

        print(
            f"Processed orders validation passed: "
            f"{len(order_parquet_files)} "
            f"Parquet file(s)"
        )

        # Validate processed customer summary
        customer_objects = get_s3_objects(
            "processed/customer_summary/"
        )

        customer_parquet_files = [
            obj["Key"]
            for obj in customer_objects
            if obj["Key"].endswith(".parquet")
        ]

        if not customer_parquet_files:
            raise ValueError(
                "Validation failed: "
                "No customer summary Parquet "
                "files found."
            )

        print(
            f"Customer summary validation passed: "
            f"{len(customer_parquet_files)} "
            f"Parquet file(s)"
        )

        print(
            "PIPELINE VALIDATION SUCCESSFUL!"
        )

    # --------------------------------------------------
    # TASK DEPENDENCIES
    # --------------------------------------------------

    (
        generate_data()
        >> upload_raw_to_s3()
        >> run_spark_pipeline()
        >> upload_processed_to_s3()
        >> validate_pipeline()
    )


retail_pipeline()