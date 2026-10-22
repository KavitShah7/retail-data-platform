import random

from faker import Faker

from generator_config import NUM_PRODUCTS, seed_random, write_csv

FIELDS = ["product_id", "product_name", "category", "price"]
CATEGORIES = ["Electronics", "Clothing", "Home", "Beauty", "Sports"]


def create_product(fake, product_id):
    return {
        "product_id": product_id,
        "product_name": fake.catch_phrase(),
        "category": random.choice(CATEGORIES),
        "price": round(random.uniform(5.0, 500.0), 2),
    }


def main():
    fake = Faker()
    seed_random(fake)

    products = [create_product(fake, i) for i in range(1, NUM_PRODUCTS + 1)]
    write_csv("products.csv", FIELDS, products)

    print(f"Generated {len(products)} products successfully!")


if __name__ == "__main__":
    main()
