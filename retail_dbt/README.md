# Retail dbt models

This project reads `RETAIL_DB.PROCESSED.ORDERS` and
`RETAIL_DB.PROCESSED.CUSTOMER_SUMMARY`, creates two staging views and the
`dim_customer` / `fact_orders` marts, and declares nine data tests.

Use the separate `.dbt-venv` with `requirements-dbt.txt`. For a real deployment,
copy `profiles.example.yml` to `profiles.yml` in a private directory and set
`DBT_PROFILES_DIR` to that directory (or use your existing `~/.dbt/profiles.yml`).
The example uses an encrypted key file and environment variables; keep secrets
outside Git. Set the key path to an absolute path. The source database/schema
are fixed in `models/staging/sources.yml`; keep them aligned with the loader.
The example writes models to `RETAIL_DB.ANALYTICS`, which requires appropriate grants.

Offline parsing, without a Snowflake connection, from the repository root:

```bash
DBT_SEND_ANONYMOUS_USAGE_STATS=false .dbt-venv/bin/dbt parse \
  --project-dir retail_dbt --profiles-dir tests/fixtures/dbt --no-partial-parse
```

The fixture is deliberately unusable for live authentication. Parsing validates
project structure and Jinja references, not warehouse SQL, grants, or data.
`dbt debug`, `dbt run`, `dbt test`, and connection-dependent compilation need
separate live validation; `run` and `test` may incur warehouse charges.
