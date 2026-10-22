import random
from datetime import datetime, timedelta

from generator_config import (
    NUM_CUSTOMERS,
    NUM_ORDERS,
    ORDER_STATUSES,
    seed_random,
    write_csv,
)

FIELDS = ["order_id", "customer_id", "order_date", "status"]


def create_order(order_id, today):
    order_date = today - timedelta(days=random.randint(0, 365))
    return {
        "order_id": order_id,
        "customer_id": random.randint(1, NUM_CUSTOMERS),
        "order_date": order_date.strftime("%Y-%m-%d"),
        "status": random.choice(ORDER_STATUSES),
    }


def main():
    seed_random()

    today = datetime.now()
    orders = [create_order(i, today) for i in range(1, NUM_ORDERS + 1)]
    write_csv("orders.csv", FIELDS, orders)

    print(f"Generated {len(orders)} orders successfully!")


if __name__ == "__main__":
    main()
