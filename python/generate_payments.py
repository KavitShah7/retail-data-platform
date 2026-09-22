import csv
import random


order_totals = {}

with open("data/order_items.csv", "r") as file:
    reader = csv.DictReader(file)

    for row in reader:
        order_id = int(row["order_id"])
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])

        item_total = quantity * unit_price

        if order_id not in order_totals:
            order_totals[order_id] = 0

        order_totals[order_id] += item_total


payments = []

payment_methods = [
    "credit_card",
    "debit_card",
    "paypal",
    "apple_pay"
]


for order_id, order_total in order_totals.items():

    payment = {
        "payment_id": order_id,
        "order_id": order_id,
        "payment_amount": round(order_total, 2),
        "payment_method": random.choice(payment_methods)
    }

    payments.append(payment)


with open("data/payments.csv", "w", newline="") as file:
    fieldnames = [
        "payment_id",
        "order_id",
        "payment_amount",
        "payment_method"
    ]

    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(payments)


print(f"Generated {len(payments)} payments successfully!")