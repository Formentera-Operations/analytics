{{
    config(
        snowflake_warehouse=set_warehouse_size('M') if target.name in ['prod', 'ci'] else target.warehouse,
    )
}}

with corporate_scenarios as (
    select *
    from {{ ref('int_economic_runs_with_one_liners') }}
    where scenario_id in (
        select scenario_id 
        from {{ ref('corporate_reserve_scenarios') }}
    )


)

select * from corporate_scenarios
