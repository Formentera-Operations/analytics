{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with marketdata as(
    select *
    from {{ ref('int_aegis__market_data') }}
)

select
    *
from marketdata
where as_of_date > '2021-12-31'