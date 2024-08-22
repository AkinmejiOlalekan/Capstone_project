{{ config(materialized='table') }}

with source as (
    SELECT
        *
    FROM{{source("my_ecommerce", "olist_order_payments_dataset")}}
)

SELECT * FROM source