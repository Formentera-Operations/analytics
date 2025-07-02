{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__reference_wells') }}
