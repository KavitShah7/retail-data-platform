FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir Faker

COPY python/generate_customers.py ./python/generate_customers.py

CMD ["python", "python/generate_customers.py"]