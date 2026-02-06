{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with marketdata as(
    select 
        market_data_id as "Market Data ID",
        pricing_index_code AS "Pricing Index Code",
        pricing_index_name AS "Pricing Index Name",
        product_type AS "Product Type",
        as_of_date AS "As Of Date",
        delivery_date AS "Delivery Date",
        price AS "Price"
    from {{ ref('int_aegis__market_data') }}
    
)
,
aggregate as (
    select
        *
       ,ROW_NUMBER() OVER (
    PARTITION BY
        "Pricing Index Code",
        "Pricing Index Name",
        "Product Type",
        "Delivery Date"
    ORDER BY "As Of Date" DESC) as rn
    from marketdata
)


select
    *
from aggregate
where rn = 1
and "Delivery Date" > '2021-12-31'
order by "Pricing Index Name", "Product Type", "Delivery Date" , "As Of Date" asc