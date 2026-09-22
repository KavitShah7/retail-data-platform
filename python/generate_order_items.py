import csv
import random


products = []

with open("data/products.csv", "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        row["product_id"] = int(row["product_id"])
        row["price"] = float(row["price"])
        products.append(row)


order_items = []
order_item_id = 1

for order_id in range(1, 1001):

    number_of_items = random.randint(1, 5)

    selected_products = random.sample(
        products,
        number_of_items
    )

    for product in selected_products:

        quantity = random.randint(1, 3)

        order_item = {
            "order_item_id": order_item_id,
            "order_id": order_id,
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": product["price"]
        }

        order_items.append(order_item)

        order_item_id += 1


with open("data/order_items.csv", "w", newline="") as file:

    fieldnames = [
        "order_item_id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price"
    ]

    writer = csv.DictWriter(file, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(order_items)


print(f"Generated {len(order_items)} order items successfully!")