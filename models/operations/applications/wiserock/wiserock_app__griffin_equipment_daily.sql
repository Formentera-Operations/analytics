{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin daily equipment data.
    Source: stg_procount__equipment_daily
*/

select * from {{ ref('stg_procount__equipment_daily') }}
