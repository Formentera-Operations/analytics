{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__daily_costs') }}