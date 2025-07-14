{{ config(
    tags=['wiserock', 'prodview']
) }}

select * from {{ ref('stg_wiserock__pv_completion_parameters') }}