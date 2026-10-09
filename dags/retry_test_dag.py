from datetime import timedelta
from pathlib import Path

import pendulum

from airflow.sdk import dag, task


RETRY_MARKER = Path("/tmp/retail_airflow_retry_test")


@dag(
    dag_id="retail_retry_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["retail-data-platform"],
)
def retry_test_pipeline():

    @task(
        retries=1,
        retry_delay=timedelta(seconds=20),
    )
    def retry_demo():

        if not RETRY_MARKER.exists():
            RETRY_MARKER.write_text("failed_once")

            print("Intentional first-attempt failure!")

            raise RuntimeError(
                "This failure is intentional to demonstrate Airflow retries."
            )

        print("Retry detected.")
        print("Task succeeded on the second attempt!")

        RETRY_MARKER.unlink()

    retry_demo()


retry_test_pipeline()