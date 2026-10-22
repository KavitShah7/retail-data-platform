FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir Faker

COPY python/generator_config.py python/generate_customers.py ./python/

CMD ["python", "python/generate_customers.py"]