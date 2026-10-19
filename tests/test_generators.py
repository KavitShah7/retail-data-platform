import csv
import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.skipif(importlib.util.find_spec("faker") is None, reason="Faker not installed")
def test_customer_generator_from_empty_directory(tmp_path):
    script = Path(__file__).resolve().parents[1] / "python/generate_customers.py"
    subprocess.run([sys.executable, str(script)], cwd=tmp_path, check=True, capture_output=True)
    with (tmp_path / "data/customers.csv").open(encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 100
    assert {int(row["customer_id"]) for row in rows} == set(range(1, 101))
    assert all(row["email"] and row["state"] for row in rows)
