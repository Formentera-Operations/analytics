{{ 
    config(
        materialized='table',
        schema='wiserock',
        tags=['wiserock', 'combo_curve']
    ) 
}}

-- Filter forecast segments to only include specific reserve forecasts
-- for the WiseRock SaaS application

with staged_forecasts as (
    
    select * 
    from {{ ref('int_cc__forecast_segments_flattened') }}
    where forecast_id in (
        select forecast_id 
        from {{ ref('reserve_forecasts') }}
        where forecast_name = '2Q25 Reserves'
    )
    
)

select * from staged_forecasts