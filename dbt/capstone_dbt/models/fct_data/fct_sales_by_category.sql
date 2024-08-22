{{ config(materialized='table') }}

SELECT
    product_category_name,
    category_sales
FROM {{ ref('int_sales_by_category') }}
