from faker import Faker

from generator_config import NUM_CUSTOMERS, seed_random, write_csv

FIELDS = ["customer_id", "first_name", "last_name", "email", "city", "state"]


def create_customer(fake, customer_id):
    return {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "city": fake.city(),
        "state": fake.state_abbr(),
    }


def main():
    fake = Faker()
    seed_random(fake)

    customers = [create_customer(fake, i) for i in range(1, NUM_CUSTOMERS + 1)]
    write_csv("customers.csv", FIELDS, customers)

    print(f"Generated {len(customers)} customers successfully!")


if __name__ == "__main__":
    main()
