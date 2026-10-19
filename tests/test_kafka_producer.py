"""Simulate Kafka delivery results without a broker or confluent-kafka install."""
from contextlib import nullcontext
import runpy
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest


@pytest.mark.parametrize("delivery_error,remaining", [("broker unavailable", 0), (None, 1), (None, 0)])
def test_delivery_outcome(monkeypatch, capsys, delivery_error, remaining):
    fake = ModuleType("confluent_kafka")

    class Producer:
        def __init__(self, config):
            self.callbacks = []

        def produce(self, **event):
            self.callbacks.append(event["callback"])

        def poll(self, timeout):
            pass

        def flush(self, timeout=None):
            message = SimpleNamespace(topic=lambda: "retail_orders", partition=lambda: 0, offset=lambda: 0)
            for callback in self.callbacks:
                callback(delivery_error, message)
            return remaining

    fake.Producer = Producer
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake)
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    script = Path(__file__).resolve().parents[1] / "python/kafka_order_producer.py"
    outcome = pytest.raises(RuntimeError, match="Kafka delivery failed") if delivery_error or remaining else nullcontext()
    with outcome:
        runpy.run_path(str(script), run_name="__main__")
    assert ("Finished producing orders." in capsys.readouterr().out) == (not delivery_error and remaining == 0)
