{{
    config(
        materialized='table'
    )
}}

with forecasts as (
    select * from {{ ref('stg_cc__forecasts') }}
),

projects as (

    select
        
        forecast_id,
        forecast_name,
        forecast_type,
        forecast_tags,
        project_id,
        run_date,
        created_at,
        updated_at

    from

        forecasts
)

select * from projects