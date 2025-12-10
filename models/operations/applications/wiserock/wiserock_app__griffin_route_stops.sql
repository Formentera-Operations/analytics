{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin route stops data.
    Source: stg_procount__route_stops
*/

select * from {{ ref('stg_procount__route_stops') }}
