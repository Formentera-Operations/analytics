{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__stimulation_fluid_systems') }}
