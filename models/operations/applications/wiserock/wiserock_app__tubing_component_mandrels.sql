{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wiserock__wv_tubing_component_mandrels') }}
