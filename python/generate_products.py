import csv
import random
from faker import Faker

fake = Faker()

categories = [
    "Electronics",
    "Clothing",
    "Home",
    "Beauty",
    "Sports"
]


def create_product(product_id):
    return {
        "product_id": product_id,
        "product_name":fake.catch_phrase(),
        "category": random.choice(categories),
        "price": round(random.uniform(5.0, 500.0), 2)
    }


products = []

for product_id in range(1, 51):
    product = create_product(product_id)
    products.append(product)


with open("data/products.csv", "w", newline="") as file:
    fieldnames = [
        "product_id",
        "product_name",
        "category",
        "price"
    ]

    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(products)

print(f"Generated {len(products)} products successfully!")