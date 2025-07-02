{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__other_in_hole_equipment') }}