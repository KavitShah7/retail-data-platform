import csv
import random
from datetime import datetime, timedelta


def create_order(order_id):
    customer_id = random.randint(1, 100)

    days_ago = random.randint(0, 365)
    order_date = datetime.now() - timedelta(days=days_ago)

    statuses = [
        "pending",
        "shipped",
        "delivered",
        "cancelled"
    ]

    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "order_date": order_date.strftime("%Y-%m-%d"),
        "status": random.choice(statuses)
    }


orders = []

for order_id in range(1, 1001):
    order = create_order(order_id)
    orders.append(order)


with open("data/orders.csv", "w", newline="") as file:
    fieldnames = [
        "order_id",
        "customer_id",
        "order_date",
        "status"
    ]

    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(orders)


print(f"Generated {len(orders)} orders successfully!")