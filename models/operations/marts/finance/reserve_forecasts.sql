{{
    config(
        materialized='table'
    )
}}

with forecasts as (
    select * from {{ ref('stg_cc__forecasts') }}
    where project_id in (
        select project_id 
        from {{ ref('corporate_reserve_scenarios') }}
    ) 
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
order by updated_at desc