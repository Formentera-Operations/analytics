{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__wellhead_components') }}
