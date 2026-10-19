"""Parse DAGs locally; never execute tasks or contact pipeline services."""
from pathlib import Path

from airflow.dag_processing.dagbag import DagBag


def main():
    bag = DagBag(dag_folder=Path(__file__).resolve().parents[1] / "dags")
    if bag.import_errors:
        raise RuntimeError(bag.import_errors)
    expected = {"retail_data_pipeline": 11, "retail_test_dag": 3, "retail_retry_test": 1}
    if set(bag.dags) != set(expected):
        raise AssertionError(f"Unexpected DAG IDs: {set(bag.dags)}")
    for name, count in expected.items():
        if len(bag.dags[name].tasks) != count:
            raise AssertionError(f"Unexpected task count for {name}")
        print(f"{name}: {count} tasks; import OK")


if __name__ == "__main__":
    main()
