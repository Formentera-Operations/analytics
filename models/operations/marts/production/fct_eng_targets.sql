{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with prodtarget as (
    select
        cc_forecast_name as "CC Forecast Name",
        prod_date as "Prod Date",
        unit_record_id as "Unit Record ID",
        target_start_date as "Target Start Date",
        target_type as "Target Type",
        target_daily_rate_gas_mcf_per_day as "Budget - Gross Gas",
        target_daily_rate_hcliq_bbl_per_day as "Budget - Gross Oil",
        target_daily_rate_water_bbl_per_day as "Budget - Gross Water",
        year(prod_date) as "Budget Year",
        (
            target_daily_rate_hcliq_bbl_per_day
            + (target_daily_rate_gas_mcf_per_day / 6)
        ) as "Budget - Gross BOE"
    from {{ ref('int_prodview__production_targets') }}
)

select
    *,
    ("Budget - Gross BOE" / 24) as "Budget - Gross BOE/hr"
from prodtarget
where
    "Prod Date" > last_day(dateadd(year, -3, current_date()), year)
    and "Target Type" = 'Budget'
order by "Prod Date" desc
