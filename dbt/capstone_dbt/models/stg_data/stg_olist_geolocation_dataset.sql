{{ config(materialized='table') }}

with source as (
    SELECT
        *
    FROM{{source("my_ecommerce", "olist_geolocation_dataset")}}
)
SELECT * FROM source