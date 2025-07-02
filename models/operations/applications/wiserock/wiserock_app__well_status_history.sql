{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__well_status_history') }}
