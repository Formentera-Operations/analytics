{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__daily_recurring_costs') }}