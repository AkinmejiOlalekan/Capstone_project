{{ config(materialized='table') }}


WITH sales AS (
    SELECT
        cd.customer_id,
        cd.customer_state,
        od.order_id,
        od.order_status
    FROM {{ ref('stg_olist_customers_dataset') }} cd
    JOIN {{ ref('stg_olist_orders_dataset') }} od
        ON cd.customer_id = od.customer_id
    WHERE od.order_status = 'delivered'
)
SELECT 
    customer_state,
    COUNT(*) as state_order
FROM sales
GROUP BY customer_state
ORDER BY state_order DESC