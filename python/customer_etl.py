import psycopg

connection = psycopg.connect(
    dbname="retail_db"
)

cursor = connection.cursor()

cursor.execute("""
    SELECT
        customer_id,
        first_name,
        last_name,
        state
    FROM customers;
""")

customers = cursor.fetchall()

transformed_customers = []

for customer in customers:
    customer_id = customer[0]
    first_name = customer[1]
    last_name = customer[2]
    state = customer[3]

    full_name = f"{first_name} {last_name}"

    transformed_customer = (
        customer_id,
        full_name,
        state
    )

    transformed_customers.append(transformed_customer)

for customer in transformed_customers:
    cursor.execute(
    """
    INSERT INTO customer_summary (
        customer_id,
        full_name,
        state
    )
    VALUES (%s, %s, %s)
    ON CONFLICT (customer_id)
    DO UPDATE SET
        full_name = EXCLUDED.full_name,
        state = EXCLUDED.state;
    """,
    customer
)


connection.commit()

print(f"Loaded {len(transformed_customers)} customers successfully!")

cursor.close()
connection.close()