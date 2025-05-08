{{
    config(
        materialized='incremental',
        unique_key=['economic_run_well_id', 'economic_run_date'],
        incremental_strategy='merge'
    )
}} 

with corporate_scenarios as (
    select *
    from {{ ref('int_economic_runs_with_one_liners') }}
    where scenario_id in (
        select scenario_id 
        from {{ ref('corporate_reserve_scenarios') }}
    )

    {% if is_incremental() %}
    and economic_run_date >= (select max(economic_run_date) from {{ this }})
    {% endif %}
)

select * from corporate_scenarios