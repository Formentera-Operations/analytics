{{ config(
    tags=['wiserock', 'prodview'],
    snowflake_warehouse=set_warehouse_size('M') if target.name in ['prod', 'ci'] else target.warehouse
) }}

select * from {{ ref('stg_wiserock__pv_daily_allocations') }}
