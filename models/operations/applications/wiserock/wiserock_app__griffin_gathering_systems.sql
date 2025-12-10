{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin gathering systems data.
    Source: stg_procount__gathering_systems
*/

select * from {{ ref('stg_procount__gathering_systems') }}
