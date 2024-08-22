{{ config(materialized='table') }}

WITH category AS (
    SELECT
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.price,
        pd.product_category_name
    FROM {{ ref('stg_olist_order_items_dataset') }} oi
    JOIN {{ ref('stg_olist_product_dataset') }} pd
        ON oi.product_id = pd.product_id
)
SELECT
    product_category_name,
    SUM(price) as category_sales 
FROM category
GROUP BY product_category_name
ORDER BY category_sales DESC