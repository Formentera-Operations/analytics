{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__job_afe_definitions') }}