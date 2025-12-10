{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin routes data.
    Source: stg_procount__routes
*/

select * from {{ ref('stg_procount__routes') }}
