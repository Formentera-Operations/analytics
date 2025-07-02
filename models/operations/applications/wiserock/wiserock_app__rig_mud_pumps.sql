{{ config(
    tags=['wiserock', 'wellview']
) }}

select * from {{ ref('stg_wellview__rig_mud_pumps') }}
