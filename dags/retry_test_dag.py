from datetime import timedelta

import pendulum

from airflow.sdk import dag, get_current_context, task



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

        if get_current_context()["ti"].try_number == 1:

            print("Intentional first-attempt failure!")

            raise RuntimeError(
                "This failure is intentional to demonstrate Airflow retries."
            )

        print("Retry detected.")
        print("Task succeeded on the second attempt!")


    retry_demo()


retry_test_pipeline()