FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Keep in sync with requirements.txt.
RUN pip install Faker==40.39.0

# Run as an unprivileged user; it owns only the output folder.
RUN useradd --create-home --uid 10001 app \
    && mkdir -p /app/data \
    && chown app:app /app/data

COPY python/generator_config.py python/generate_customers.py ./python/

USER app

CMD ["python", "python/generate_customers.py"]
