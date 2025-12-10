{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin route stop items data.
    Source: stg_procount__route_stop_items
*/

select * from {{ ref('stg_procount__route_stop_items') }}
