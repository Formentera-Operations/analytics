{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wiserock__wv_daily_costs') }}