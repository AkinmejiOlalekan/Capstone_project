{{ config(materialized='table') }}

with source as (
    SELECT
        *
    FROM{{source("my_ecommerce", "olist_order_reviews_dataset")}}
)
SELECT * FROM source