import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


TOPIC = "retail_orders"
delivery_errors = []

producer = Producer({
    "bootstrap.servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092"
    )
})

def delivery_report(err, msg):
    if err is not None:
        delivery_errors.append(err)
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered to topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


for order_id in range(1001, 1011):
    order_event = {
        "order_id": order_id,
        "customer_id": random.randint(1, 100),
        "order_timestamp": datetime.now(timezone.utc).isoformat(),
        "status": random.choice(
            ["pending", "shipped", "delivered", "cancelled"]
        ),
        "order_total": round(random.uniform(10, 500), 2)
    }

    producer.produce(
        topic=TOPIC,
        key=str(order_event["customer_id"]),
        value=json.dumps(order_event),
        callback=delivery_report
    )

    producer.poll(0)

    print(f"Produced: {order_event}")

    time.sleep(2)


remaining = producer.flush(30)

if remaining or delivery_errors:
    raise RuntimeError(
        f"Kafka delivery failed: {len(delivery_errors)} error(s), "
        f"{remaining} message(s) still queued after 30 seconds."
    )

print("Finished producing orders.")