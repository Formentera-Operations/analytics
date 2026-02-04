{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with marketdata as(
    select 
        pricing_index_code AS "Pricing Index Code",
        pricing_index_name AS "Pricing Index Name",
        product_type AS "Product Type",
        greatest(as_of_date) AS "As Of Date",
        delivery_date AS "Delivery Date",
        price AS "Price"
    from {{ ref('int_aegis__market_data') }}
    group by all
)

select
    *
from marketdata
where "Delivery Date" > '2021-12-31'