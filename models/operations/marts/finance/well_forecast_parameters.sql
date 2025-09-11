{{ 
    config(
        materialized='table',
        tags=['forecasts', 'combo_curve']
    ) 
}}

-- Filter forecast segments to only include specific reserve forecasts
-- for the WiseRock SaaS application

with staged_forecasts as (
    
    select * 
    from {{ ref('int_cc__forecast_segments_flattened') }}
    
    
)

select * from staged_forecasts
