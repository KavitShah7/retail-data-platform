"""These tests import DAGs and mock every task operation they invoke."""
import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

pytest.importorskip("airflow.sdk")
from airflow.dag_processing.dagbag import DagBag

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def pipeline(monkeypatch):
    monkeypatch.delenv("RETAIL_PROJECT_ROOT", raising=False)
    spec = importlib.util.spec_from_file_location("pipeline_under_test", ROOT / "dags/retail_pipeline_dag.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def task_callable(module, task_id):
    return module.retail_pipeline().get_task(task_id).python_callable


def test_imports_and_pipeline_dependencies():
    bag = DagBag(dag_folder=ROOT / "dags")
    assert not bag.import_errors
    assert set(bag.dags) == {"retail_data_pipeline", "retail_test_dag", "retail_retry_test"}
    dag = bag.dags["retail_data_pipeline"]
    assert len(dag.tasks) == 11
    assert dag.max_active_runs == 1
    assert dag.get_task("load_snowflake_data").upstream_task_ids == {
        "validate_raw_data", "validate_orders", "validate_customer_summary"
    }
    assert dag.get_task("pipeline_complete").upstream_task_ids == {"test_dbt_models"}


def test_project_root_follows_repository(pipeline):
    assert pipeline.PROJECT_ROOT == ROOT


def test_no_s3_changes_if_any_dataset_missing(pipeline, monkeypatch, tmp_path):
    (tmp_path / "processed/orders").mkdir(parents=True)
    (tmp_path / "processed/orders/part.parquet").touch()
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    run = Mock()
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    with pytest.raises(FileNotFoundError):
        task_callable(pipeline, "upload_processed_to_s3")()
    run.assert_not_called()


def test_failed_upload_never_starts_s3_delete(pipeline, monkeypatch, tmp_path):
    for dataset in ("orders", "customer_summary"):
        folder = tmp_path / "processed" / dataset
        folder.mkdir(parents=True)
        (folder / "part.parquet").touch()
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    run = Mock(side_effect=[SimpleNamespace(returncode=0), subprocess.CalledProcessError(1, "aws")])
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    with pytest.raises(subprocess.CalledProcessError):
        task_callable(pipeline, "upload_processed_to_s3")()
    assert run.call_count == 2
    assert all("--delete" not in call.args[0] for call in run.call_args_list)


def test_retry_demo_uses_airflow_attempt_number(monkeypatch):
    spec = importlib.util.spec_from_file_location("retry_under_test", ROOT / "dags/retry_test_dag.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    callback = module.retry_test_pipeline().get_task("retry_demo").python_callable
    monkeypatch.setattr(module, "get_current_context", lambda: {"ti": SimpleNamespace(try_number=1)})
    with pytest.raises(RuntimeError, match="intentional"):
        callback()
    monkeypatch.setattr(module, "get_current_context", lambda: {"ti": SimpleNamespace(try_number=2)})
    callback()
