SELECT
    order_id,
    customer_id,
    order_date,
    status,
    order_total
FROM {{ source('processed', 'orders') }}
