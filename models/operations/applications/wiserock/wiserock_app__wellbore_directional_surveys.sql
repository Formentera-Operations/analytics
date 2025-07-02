{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__wellbore_directional_surveys') }}
