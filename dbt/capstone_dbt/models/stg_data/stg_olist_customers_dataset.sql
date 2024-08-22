{{ config(materialized='table') }}

with source as (
    SELECT
        *
    FROM{{source("my_ecommerce", "olist_customers_dataset")}}
)

SELECT * FROM source