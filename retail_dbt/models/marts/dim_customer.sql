SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state
FROM {{ ref('stg_customers') }}
