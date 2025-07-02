{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__tubing_components') }}
