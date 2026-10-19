
import json
import os
import subprocess

from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(
    os.environ.get("RETAIL_PROJECT_ROOT", Path(__file__).resolve().parents[1])
).expanduser().resolve()

# Python environment for data generation and Spark
PROJECT_PYTHON = Path(
    os.environ.get("RETAIL_PROJECT_PYTHON", PROJECT_ROOT / ".venv/bin/python")
).expanduser()

# Python environment for Snowflake connector
AIRFLOW_PYTHON = Path(
    os.environ.get("RETAIL_SNOWFLAKE_PYTHON", PROJECT_ROOT / ".airflow-venv/bin/python")
).expanduser()

# dbt configuration
DBT_EXECUTABLE = Path(
    os.environ.get("RETAIL_DBT_EXECUTABLE", PROJECT_ROOT / ".dbt-venv/bin/dbt")
).expanduser()
DBT_PROJECT = PROJECT_ROOT / "retail_dbt"

# S3 configuration
RETAIL_BUCKET = os.environ.get("RETAIL_BUCKET", "retail-data-platform-kavit-2026")


# ==================================================
# 2. AIRFLOW SETTINGS
# ==================================================

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# ==================================================
# 3. HELPER FUNCTIONS
# ==================================================

def run_python_script(script_name):
    """
    Execute a Python script using the project's
    main virtual environment.
    """

    script_path = PROJECT_ROOT / "python" / script_name

    subprocess.run(
        [
            str(PROJECT_PYTHON),
            str(script_path),
        ],
        cwd=PROJECT_ROOT,
        check=True,
    )


def get_s3_objects(prefix):
    """
    Return objects found under an S3 prefix.
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


# ==================================================
# 4. AIRFLOW DAG
# ==================================================

@dag(
    dag_id="retail_data_pipeline",
    schedule=None,
    start_date=pendulum.datetime(
        2026, 1, 1, tz="UTC"
    ),
    catchup=False,
    # Tasks share local outputs and replace fixed S3/Snowflake datasets.
    max_active_runs=1,
    default_args=default_args,
    tags=["retail-data-platform"],
)
def retail_pipeline():

    # ----------------------------------------------
    # TASK 1: GENERATE SYNTHETIC DATA
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

            print(f"Generating data: {script}")

            run_python_script(script)

        print("All datasets generated successfully.")


    # ----------------------------------------------
    # TASK 2: UPLOAD RAW CSV FILES TO S3
    # ----------------------------------------------

    @task
    def upload_raw_to_s3():

        env = os.environ.copy()
        env["RETAIL_BUCKET"] = RETAIL_BUCKET

        script_path = (
            PROJECT_ROOT / "python" / "upload_to_s3.py"
        )

        subprocess.run(
            [
                str(PROJECT_PYTHON),
                str(script_path),
            ],
            cwd=PROJECT_ROOT,
            env=env,
            check=True,
        )

        print("Raw CSV files uploaded to S3.")


    # ----------------------------------------------
    # TASK 3: EXECUTE PYSPARK TRANSFORMATIONS
    # ----------------------------------------------

    @task
    def run_spark_pipeline():

        run_python_script(
            "retail_spark_pipeline.py"
        )

        print("PySpark transformations completed.")


    # ----------------------------------------------
    # TASK 4: UPLOAD PROCESSED PARQUET TO S3
    # ----------------------------------------------

    @task
    def upload_processed_to_s3():

        datasets = [
            "orders",
            "customer_summary",
        ]

        # Validate both local datasets before
        # modifying anything in S3.

        for dataset in datasets:

            local_source = (
                PROJECT_ROOT / "processed" / dataset
            )

            if not local_source.is_dir():
                raise FileNotFoundError(
                    f"Dataset directory missing: {local_source}"
                )

            parquet_files = list(
                local_source.rglob("*.parquet")
            )

            if not parquet_files:
                raise FileNotFoundError(
                    f"No Parquet files found: {local_source}"
                )

            print(
                f"{dataset}: found "
                f"{len(parquet_files)} Parquet file(s)"
            )

        # Phase 1:
        # Upload new Parquet files before deleting
        # any existing objects.

        for dataset in datasets:

            local_source = (
                PROJECT_ROOT / "processed" / dataset
            )

            s3_destination = (
                f"s3://{RETAIL_BUCKET}"
                f"/processed/{dataset}/"
            )

            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    str(local_source),
                    s3_destination,
                    "--exclude",
                    "*",
                    "--include",
                    "*.parquet",
                ],
                check=True,
            )

            print(f"Uploaded {dataset} Parquet files.")

        # Phase 2:
        # After both uploads succeed, remove old
        # Parquet files that no longer exist locally.

        for dataset in datasets:

            local_source = (
                PROJECT_ROOT / "processed" / dataset
            )

            s3_destination = (
                f"s3://{RETAIL_BUCKET}"
                f"/processed/{dataset}/"
            )

            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    str(local_source),
                    s3_destination,
                    "--exclude",
                    "*",
                    "--include",
                    "*.parquet",
                    "--delete",
                ],
                check=True,
            )

        print("Processed Parquet synchronized to S3.")


    # ----------------------------------------------
    # TASK 5A: VALIDATE RAW DATA IN S3
    # ----------------------------------------------

    @task
    def validate_raw_data():

        expected_files = [
            "raw/customers/customers.csv",
            "raw/products/products.csv",
            "raw/orders/orders.csv",
            "raw/order_items/order_items.csv",
            "raw/payments/payments.csv",
        ]

        for key in expected_files:

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

            print(f"Validated raw file: {key}")

        print("Raw S3 validation passed.")


    # ----------------------------------------------
    # TASK 5B: VALIDATE ORDERS PARQUET
    # ----------------------------------------------

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
                "No orders Parquet files found in S3."
            )

        print(
            f"Orders validation passed: "
            f"{len(parquet_files)} Parquet file(s)."
        )


    # ----------------------------------------------
    # TASK 5C: VALIDATE CUSTOMER SUMMARY PARQUET
    # ----------------------------------------------

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
                "No customer summary Parquet files "
                "found in S3."
            )

        print(
            f"Customer summary validation passed: "
            f"{len(parquet_files)} Parquet file(s)."
        )


    # ----------------------------------------------
    # TASK 6: LOAD DATA INTO SNOWFLAKE
    # ----------------------------------------------

    @task
    def load_snowflake_data():

        if not os.environ.get(
            "SNOWFLAKE_KEY_PASSPHRASE"
        ):
            raise RuntimeError(
                "SNOWFLAKE_KEY_PASSPHRASE is not set "
                "in the Airflow environment."
            )

        script_path = (
            PROJECT_ROOT / "python" / "load_snowflake.py"
        )

        subprocess.run(
            [
                str(AIRFLOW_PYTHON),
                str(script_path),
            ],
            cwd=PROJECT_ROOT,
            check=True,
        )

        print("Snowflake data loaded successfully.")


    # ----------------------------------------------
    # TASK 7: EXECUTE DBT MODELS
    # ----------------------------------------------

    @task
    def run_dbt_models():

        if not os.environ.get(
            "SNOWFLAKE_KEY_PASSPHRASE"
        ):
            raise RuntimeError(
                "Snowflake key passphrase is missing."
            )

        subprocess.run(
            [
                str(DBT_EXECUTABLE),
                "run",
                "--project-dir",
                str(DBT_PROJECT),
            ],
            cwd=DBT_PROJECT,
            check=True,
        )

        print("dbt models built successfully.")


    # ----------------------------------------------
    # TASK 8: EXECUTE DBT TESTS
    # ----------------------------------------------

    @task
    def test_dbt_models():

        subprocess.run(
            [
                str(DBT_EXECUTABLE),
                "test",
                "--project-dir",
                str(DBT_PROJECT),
            ],
            cwd=DBT_PROJECT,
            check=True,
        )

        print("All dbt tests passed.")


    # ----------------------------------------------
    # TASK 9: PIPELINE COMPLETE
    # ----------------------------------------------

    @task
    def pipeline_complete():

        print("=" * 50)
        print("RETAIL DATA PLATFORM")
        print("END-TO-END PIPELINE COMPLETED SUCCESSFULLY")
        print("=" * 50)


    # ==============================================
    # 5. CREATE AIRFLOW TASK INSTANCES
    # ==============================================

    generate_task = generate_data()

    upload_raw_task = upload_raw_to_s3()

    spark_task = run_spark_pipeline()

    upload_processed_task = upload_processed_to_s3()

    validate_raw_task = validate_raw_data()

    validate_orders_task = validate_orders()

    validate_customer_task = validate_customer_summary()

    snowflake_task = load_snowflake_data()

    dbt_run_task = run_dbt_models()

    dbt_test_task = test_dbt_models()

    complete_task = pipeline_complete()


    # ==============================================
    # 6. DEFINE TASK DEPENDENCIES
    # ==============================================

    # Step 1: Generate, transform and upload data

    (
        generate_task
        >> upload_raw_task
        >> spark_task
        >> upload_processed_task
    )

    # Step 2: Validate three S3 datasets in parallel

    upload_processed_task >> [
        validate_raw_task,
        validate_orders_task,
        validate_customer_task,
    ]

    # Step 3: Load Snowflake, run dbt and test

    (
        [
            validate_raw_task,
            validate_orders_task,
            validate_customer_task,
        ]
        >> snowflake_task
        >> dbt_run_task
        >> dbt_test_task
        >> complete_task
    )


# ==================================================
# 7. REGISTER THE AIRFLOW DAG
# ==================================================

retail_pipeline()
