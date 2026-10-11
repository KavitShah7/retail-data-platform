SELECT
    customer_id,
    first_name,
    last_name,
    email,
    city,
    state,
    order_count,
    total_revenue
FROM {{ source('processed', 'customer_summary') }}
