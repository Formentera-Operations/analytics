{{
  config(
    materialized='table',
    tags=['forecast', 'eur']
  )
}}

with forecast_params as (
    select
        well_id,
        project_id,
        forecast_id,
        forecast_date,
        product_phase,
        total_eur
    from {{ ref('well_forecast_parameters') }}
),

pivoted_forecast as (
    select
        well_id,
        project_id,
        forecast_id,
        max(case when product_phase = 'water' then forecast_date end) as water_forecast_date,
        max(case when product_phase = 'water' then total_eur end) as water_eur,
        max(case when product_phase = 'oil' then forecast_date end) as oil_forecast_date,
        max(case when product_phase = 'oil' then total_eur end) as oil_eur,
        max(case when product_phase = 'gas' then forecast_date end) as gas_forecast_date,
        max(case when product_phase = 'gas' then total_eur end) as gas_eur
    from forecast_params
    group by well_id, project_id, forecast_id
),

final as (
    select 
        left(w.api_14, 10) as api,
        w.well_name,
        pf.project_id,
        pf.forecast_id,
        date(pf.water_forecast_date) as water_date,
        pf.water_eur,
        date(pf.oil_forecast_date) as oil_date,
        pf.oil_eur,
        date(pf.gas_forecast_date) as gas_date,
        pf.gas_eur
    from {{ ref('wells') }} as w
    inner join pivoted_forecast as pf
        on w.well_id = pf.well_id
)

select * from final