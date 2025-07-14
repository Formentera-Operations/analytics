{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wiserock__wv_casing_components') }}