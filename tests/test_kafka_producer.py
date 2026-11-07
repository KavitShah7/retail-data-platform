"""Simulate Kafka delivery results without a broker or confluent-kafka install."""
import logging
import runpy
import sys
from contextlib import nullcontext
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "python/kafka_order_producer.py"


def install_fake_kafka(monkeypatch, delivery_error=None, remaining=0):
    fake = ModuleType("confluent_kafka")
    created = []

    class Producer:
        def __init__(self, config):
            self.config = config
            self.events = []
            created.append(self)

        def produce(self, **event):
            self.events.append(event)

        def poll(self, timeout):
            pass

        def flush(self, timeout=None):
            message = SimpleNamespace(topic=lambda: "retail_orders", partition=lambda: 0, offset=lambda: 0)
            for event in self.events:
                event["callback"](delivery_error, message)
            return remaining

    fake.Producer = Producer
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake)
    monkeypatch.setattr("time.sleep", lambda seconds: None)
    return created


@pytest.mark.parametrize("delivery_error,remaining", [("broker unavailable", 0), (None, 1), (None, 0)])
def test_delivery_outcome(monkeypatch, caplog, delivery_error, remaining):
    install_fake_kafka(monkeypatch, delivery_error, remaining)
    caplog.set_level(logging.INFO)
    should_fail = bool(delivery_error or remaining)
    outcome = pytest.raises(RuntimeError, match="Kafka delivery failed") if should_fail else nullcontext()
    with outcome:
        runpy.run_path(str(SCRIPT), run_name="__main__")
    assert ("Finished producing" in caplog.text) == (not should_fail)


def test_producer_is_idempotent_and_configurable(monkeypatch):
    created = install_fake_kafka(monkeypatch)
    monkeypatch.setenv("KAFKA_TOPIC", "custom_topic")
    monkeypatch.setenv("KAFKA_NUM_EVENTS", "3")
    monkeypatch.setenv("KAFKA_START_ORDER_ID", "50")
    runpy.run_path(str(SCRIPT), run_name="__main__")

    producer = created[0]
    assert producer.config["acks"] == "all"
    assert producer.config["enable.idempotence"] is True
    assert [e["topic"] for e in producer.events] == ["custom_topic"] * 3
    assert [e["value"] for e in producer.events][0].startswith('{"order_id": 50,')
