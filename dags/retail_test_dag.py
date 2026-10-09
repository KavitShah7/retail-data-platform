import pendulum

from airflow.sdk import dag, task


@dag(
    dag_id="retail_test_dag",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["retail-data-platform"],
)
def retail_test_pipeline():

    @task
    def start():
        print("Retail pipeline started!")

    @task
    def check_data():
        print("Data check completed!")

    @task
    def finish():
        print("Retail pipeline finished successfully!")

    start() >> check_data() >> finish()


retail_test_pipeline()