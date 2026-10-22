import random

from generator_config import read_csv, seed_random, write_csv

FIELDS = ["payment_id", "order_id", "payment_amount", "payment_method"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay"]


def main():
    seed_random()

    # One payment per order, for the full order total.
    order_totals = {}
    for row in read_csv("order_items.csv"):
        order_id = int(row["order_id"])
        item_total = int(row["quantity"]) * float(row["unit_price"])
        order_totals[order_id] = order_totals.get(order_id, 0) + item_total

    payments = [
        {
            "payment_id": order_id,
            "order_id": order_id,
            "payment_amount": round(total, 2),
            "payment_method": random.choice(PAYMENT_METHODS),
        }
        for order_id, total in order_totals.items()
    ]
    write_csv("payments.csv", FIELDS, payments)

    print(f"Generated {len(payments)} payments successfully!")


if __name__ == "__main__":
    main()
