"""Publish synthetic retail order events to Kafka.

Environment overrides:
    KAFKA_BOOTSTRAP_SERVERS   default localhost:9092
    KAFKA_TOPIC               default retail_orders
    KAFKA_NUM_EVENTS          default 10
    KAFKA_START_ORDER_ID      default 1001
    KAFKA_EVENT_INTERVAL_SECONDS  pause between events, default 2
    KAFKA_FLUSH_TIMEOUT_SECONDS   default 30
"""
import json
import logging
import os
import random
import time
from datetime import UTC, datetime

from confluent_kafka import Producer

ORDER_STATUSES = ["pending", "shipped", "delivered", "cancelled"]

log = logging.getLogger("kafka_order_producer")


def build_producer(bootstrap_servers):
    return Producer({
        "bootstrap.servers": bootstrap_servers,
        # Wait for all in-sync replicas and let the client dedupe its own retries.
        "acks": "all",
        "enable.idempotence": True,
        "client.id": "retail-order-producer",
    })


def make_order_event(order_id):
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, 100),
        "order_timestamp": datetime.now(UTC).isoformat(),
        "status": random.choice(ORDER_STATUSES),
        "order_total": round(random.uniform(10, 500), 2),
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    topic = os.getenv("KAFKA_TOPIC", "retail_orders")
    num_events = int(os.getenv("KAFKA_NUM_EVENTS", "10"))
    start_id = int(os.getenv("KAFKA_START_ORDER_ID", "1001"))
    interval = float(os.getenv("KAFKA_EVENT_INTERVAL_SECONDS", "2"))
    flush_timeout = float(os.getenv("KAFKA_FLUSH_TIMEOUT_SECONDS", "30"))

    producer = build_producer(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    delivery_errors = []

    def delivery_report(err, msg):
        if err is not None:
            delivery_errors.append(err)
            log.error("Delivery failed: %s", err)
        else:
            log.info("Delivered to %s [%s] @ offset %s", msg.topic(), msg.partition(), msg.offset())

    for order_id in range(start_id, start_id + num_events):
        event = make_order_event(order_id)
        producer.produce(
            topic=topic,
            key=str(event["customer_id"]),
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        log.info("Produced order %s", order_id)
        time.sleep(interval)

    remaining = producer.flush(flush_timeout)
    if remaining or delivery_errors:
        raise RuntimeError(
            f"Kafka delivery failed: {len(delivery_errors)} error(s), "
            f"{remaining} message(s) still queued after {flush_timeout:g} seconds."
        )
    log.info("Finished producing %d orders.", num_events)


if __name__ == "__main__":
    main()
