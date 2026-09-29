import psycopg

connection = psycopg.connect(
    dbname="retail_db"
)

cursor = connection.cursor()
cursor.execute("SELECT COUNT(*) FROM customers;")
result = cursor.fetchone()
print(f"Number of customers in the database: {result[0]}")

cursor.close()
connection.close()
