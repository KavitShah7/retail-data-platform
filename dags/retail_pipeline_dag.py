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

# S3 configuration (required when an S3 task runs, not at DAG import)
RETAIL_BUCKET = os.environ.get("RETAIL_BUCKET", "")

GENERATOR_SCRIPTS = [
    "generate_customers.py",
    "generate_products.py",
    "generate_orders.py",
    "generate_order_items.py",
    "generate_payments.py",
]

RAW_FILES = [
    "raw/customers/customers.csv",
    "raw/products/products.csv",
    "raw/orders/orders.csv",
    "raw/order_items/order_items.csv",
    "raw/payments/payments.csv",
]

PROCESSED_DATASETS = ["orders", "customer_summary"]


# ==================================================
# 2. HELPERS
# ==================================================

def run(command, cwd=None, env=None, capture=False):
    """Run a command and fail the task if it exits non-zero."""
    return subprocess.run(
        [str(part) for part in command],
        cwd=cwd or PROJECT_ROOT,
        env=env,
        check=True,
        capture_output=capture,
        text=capture,
    )


def run_script(script_name, python=None, env=None):
    """Run a script from python/ with the given interpreter."""
    run([python or PROJECT_PYTHON, PROJECT_ROOT / "python" / script_name], env=env)


def bucket():
    if not RETAIL_BUCKET:
        raise RuntimeError("RETAIL_BUCKET is not set in the Airflow environment.")
    return RETAIL_BUCKET


def require_snowflake_env():
    for name in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_KEY_PASSPHRASE"):
        if not os.environ.get(name):
            raise RuntimeError(f"{name} is not set in the Airflow environment.")


def list_s3_objects(prefix):
    """Return {key: size} for objects under an S3 prefix."""
    result = run(
        ["aws", "s3api", "list-objects-v2", "--bucket", bucket(),
         "--prefix", prefix, "--output", "json"],
        capture=True,
    )
    contents = json.loads(result.stdout or "{}").get("Contents", [])
    return {obj["Key"]: obj.get("Size", 0) for obj in contents}


def sync_processed(dataset, delete):
    """Sync one local Parquet dataset to S3, optionally pruning old files."""
    command = [
        "aws", "s3", "sync",
        PROJECT_ROOT / "processed" / dataset,
        f"s3://{bucket()}/processed/{dataset}/",
        "--exclude", "*", "--include", "*.parquet",
    ]
    if delete:
        command.append("--delete")
    run(command)


def run_dbt(command):
    require_snowflake_env()
    run([DBT_EXECUTABLE, command, "--project-dir", DBT_PROJECT], cwd=DBT_PROJECT)


# ==================================================
# 3. AIRFLOW DAG
# ==================================================

@dag(
    dag_id="retail_data_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    # Tasks share local outputs and replace fixed S3/Snowflake datasets.
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["retail-data-platform"],
)
def retail_pipeline():

    @task
    def generate_data():
        for script in GENERATOR_SCRIPTS:
            print(f"Generating data: {script}")
            run_script(script)

    @task
    def upload_raw_to_s3():
        run_script("upload_to_s3.py", env={**os.environ, "RETAIL_BUCKET": bucket()})

    @task
    def run_spark_pipeline():
        run_script("retail_spark_pipeline.py")

    @task
    def upload_processed_to_s3():
        # Check every local dataset before touching S3.
        for dataset in PROCESSED_DATASETS:
            folder = PROJECT_ROOT / "processed" / dataset
            if not folder.is_dir():
                raise FileNotFoundError(f"Dataset directory missing: {folder}")
            files = list(folder.rglob("*.parquet"))
            if not files:
                raise FileNotFoundError(f"No Parquet files found: {folder}")
            print(f"{dataset}: found {len(files)} Parquet file(s)")

        # Upload everything first, then prune old files only once all uploads succeed.
        for dataset in PROCESSED_DATASETS:
            sync_processed(dataset, delete=False)
        for dataset in PROCESSED_DATASETS:
            sync_processed(dataset, delete=True)

    @task
    def validate_raw_data():
        found = list_s3_objects("raw/")
        missing = [key for key in RAW_FILES if not found.get(key)]
        if missing:
            raise ValueError(f"Raw files missing or empty in S3: {missing}")
        print(f"Raw S3 validation passed: {len(RAW_FILES)} files.")

    @task
    def validate_parquet(dataset):
        found = list_s3_objects(f"processed/{dataset}/")
        parquet = [key for key, size in found.items() if key.endswith(".parquet") and size > 0]
        if not parquet:
            raise ValueError(f"No non-empty {dataset} Parquet files found in S3.")
        print(f"{dataset} validation passed: {len(parquet)} Parquet file(s).")

    @task
    def load_snowflake_data():
        require_snowflake_env()
        run_script("load_snowflake.py", python=AIRFLOW_PYTHON)

    @task
    def run_dbt_models():
        run_dbt("run")

    @task
    def test_dbt_models():
        run_dbt("test")

    @task
    def pipeline_complete():
        print("Retail data platform: end-to-end pipeline completed successfully.")

    # ==============================================
    # 4. TASK DEPENDENCIES
    # ==============================================

    upload_processed = upload_processed_to_s3()
    generate_data() >> upload_raw_to_s3() >> run_spark_pipeline() >> upload_processed

    validations = [
        validate_raw_data(),
        validate_parquet.override(task_id="validate_orders")("orders"),
        validate_parquet.override(task_id="validate_customer_summary")("customer_summary"),
    ]
    upload_processed >> validations
    validations >> load_snowflake_data() >> run_dbt_models() >> test_dbt_models() >> pipeline_complete()


# ==================================================
# 5. REGISTER DAG
# ==================================================

retail_pipeline()
