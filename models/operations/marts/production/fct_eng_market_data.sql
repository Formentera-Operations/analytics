{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}
--rename column names for reporting 
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

---sort and number by most recent price by "As of Date" for each month
rankmarketdata as (
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
,
---create a y-grade calcuation for NGL prices using component % from Gas Marketing
product_multipliers AS (
    SELECT * FROM VALUES
        ('Ethane',              0.314),         --C2
        ('Propane',             0.324),         --C3
        ('IsoButane',           0.044),         --iC4
        ('Normal Butane',       0.142),         --nC3
        ('Natural Gasoline',    0.166 + .009)   --C5+   Condy
    AS t("Product Type", multiplier)
)
,

ngl_comp AS (
    SELECT
    md.*,
    md."Price" * pm.multiplier AS "NGL y-grade Price"
FROM rankmarketdata md
LEFT JOIN product_multipliers pm
    ON md."Product Type" = pm."Product Type"
)
    
select
    *
from ngl_comp
where rn = 1
and "Delivery Date" > LAST_DAY(DATEADD(year, -3,CURRENT_DATE()), year)
order by "Pricing Index Name", "Product Type", "Delivery Date" , "As Of Date" asc