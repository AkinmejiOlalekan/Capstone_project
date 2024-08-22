{{ config(materialized='table') }}

SELECT
    customer_state,
    state_order
FROM {{ ref('int_orders_by_state') }}
