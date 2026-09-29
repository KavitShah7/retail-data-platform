-- ============================================
-- 1. VIEW SAMPLE CUSTOMERS
-- ============================================

SELECT *
FROM customers
LIMIT 5;


-- ============================================
-- 2. VIEW ORDERS WITH CUSTOMER INFORMATION
-- ============================================

SELECT
    o.order_id,
    o.order_date,
    o.status,
    c.first_name,
    c.last_name
FROM orders o
JOIN customers c
    ON o.customer_id = c.customer_id
LIMIT 10;


-- ============================================
-- 3. CALCULATE TOTAL VALUE OF EACH ORDER
-- ============================================

SELECT
    order_id,
    SUM(quantity * unit_price) AS order_total
FROM order_items
GROUP BY order_id
ORDER BY order_id
LIMIT 10;


-- ============================================
-- 4. COMPARE PAYMENTS WITH CALCULATED TOTALS
-- ============================================

SELECT
    p.order_id,
    p.payment_amount,
    SUM(oi.quantity * oi.unit_price) AS calculated_total
FROM payments p
JOIN order_items oi
    ON p.order_id = oi.order_id
GROUP BY p.order_id, p.payment_amount
ORDER BY p.order_id
LIMIT 10;


-- ============================================
-- 5. FIND INCORRECT PAYMENT AMOUNTS
-- ============================================

SELECT
    p.order_id,
    p.payment_amount,
    SUM(oi.quantity * oi.unit_price) AS calculated_total
FROM payments p
JOIN order_items oi
    ON p.order_id = oi.order_id
GROUP BY p.order_id, p.payment_amount
HAVING p.payment_amount != SUM(oi.quantity * oi.unit_price);

-- ============================================
-- 6. WHICH ORDERS WERE DELIVERED
-- ============================================


SELECT order_id, customer_id, order_date, status
FROM orders
WHERE status = 'delivered'
LIMIT 10;

-- 7. Top 10 customers by number of orders
SELECT
    customer_id,
    COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 10;

--8. Customers with more than 10 orders

SELECT
    customer_id,
    COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 10
ORDER BY order_count DESC;

--9. Customers with more than 3 delivered orders

SELECT
    customer_id,
    COUNT(*) AS delivered_order_count
FROM orders
WHERE status = 'delivered'
GROUP BY customer_id
HAVING COUNT(*) > 3
ORDER BY delivered_order_count DESC;



-- 10. Categorize orders by value
SELECT
    order_id,
    SUM(quantity * unit_price) AS order_total,
    CASE
        WHEN SUM(quantity * unit_price) < 500 THEN 'low_value'
        WHEN SUM(quantity * unit_price) <= 1000 THEN 'medium_value'
        ELSE 'high_value'
    END AS order_category
FROM order_items
GROUP BY order_id
ORDER BY order_total DESC;




-- 11. Categorize orders by value (using CTE)
WITH order_totals AS (
    SELECT
        order_id,
        SUM(quantity * unit_price) AS order_total
    FROM order_items
    GROUP BY order_id
)

SELECT
    order_id,
    order_total,
    CASE
        WHEN order_total < 500 THEN 'low_value'
        WHEN order_total <= 1000 THEN 'medium_value'
        ELSE 'high_value'
    END AS order_category
FROM order_totals;


--12. Count orders by category
WITH order_totals AS (
    SELECT
        order_id,
        SUM(quantity * unit_price) AS order_total
    FROM order_items
    GROUP BY order_id
),

categorized_orders AS (
    SELECT
        order_id,
        order_total,
        CASE
            WHEN order_total < 500 THEN 'low_value'
            WHEN order_total <= 1000 THEN 'medium_value'
            ELSE 'high_value'
        END AS order_category
    FROM order_totals
)

SELECT
    order_category,
    COUNT(*) AS order_count
FROM categorized_orders
GROUP BY order_category;

--13. Find the most recent order for each customer
WITH ranked_orders AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY order_date DESC
        ) AS order_number
    FROM orders
)

SELECT
    customer_id,
    order_id,
    order_date
FROM ranked_orders
WHERE order_number = 1;


--14.
WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
)

SELECT
    customer_id,
    order_count,
    RANK() OVER (
        ORDER BY order_count DESC
    ) AS customer_rank
FROM customer_orders;

--15.