from db_connection import get_connection

connection = get_connection()

cursor = connection.cursor()
cursor.execute("""
    SELECT
        customer_id,
        first_name,
        last_name,
        state
    FROM customers
    LIMIT 10;
""")

customers = cursor.fetchall()

for customer in customers:
    customer_id = customer[0]
    first_name = customer[1]
    last_name = customer[2]
    state = customer[3]

    full_name = f"{first_name} {last_name}"

    print(customer_id, full_name, state)

cursor.close()
connection.close()  
