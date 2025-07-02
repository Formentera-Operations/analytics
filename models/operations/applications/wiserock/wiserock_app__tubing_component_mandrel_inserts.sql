{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__tubing_component_mandrel_inserts') }}
