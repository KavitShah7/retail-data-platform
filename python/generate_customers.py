import csv
from pathlib import Path
from faker import Faker


fake = Faker()


def create_customer(customer_id):
    return {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.state_abbr(),
    }

customers=[]

for customer_id in range(1,101):
    customer = create_customer(customer_id)
    customers.append(customer)

Path("data").mkdir(parents=True, exist_ok=True)

with open("data/customers.csv", "w", newline="", encoding="utf-8") as file:
    fieldnames = [
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state"
    ]

    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(customers)

print(f"Generated {len(customers)} customers successfully!")