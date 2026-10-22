import random

from generator_config import NUM_ORDERS, read_csv, seed_random, write_csv

FIELDS = ["order_item_id", "order_id", "product_id", "quantity", "unit_price"]


def main():
    seed_random()

    products = [
        {"product_id": int(row["product_id"]), "price": float(row["price"])}
        for row in read_csv("products.csv")
    ]

    order_items = []
    for order_id in range(1, NUM_ORDERS + 1):
        for product in random.sample(products, random.randint(1, 5)):
            order_items.append({
                "order_item_id": len(order_items) + 1,
                "order_id": order_id,
                "product_id": product["product_id"],
                "quantity": random.randint(1, 3),
                "unit_price": product["price"],
            })

    write_csv("order_items.csv", FIELDS, order_items)

    print(f"Generated {len(order_items)} order items successfully!")


if __name__ == "__main__":
    main()
