{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin connections data.
    Source: stg_procount__connections
*/

select * from {{ ref('stg_procount__connections') }}
