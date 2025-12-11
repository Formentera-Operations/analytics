{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin daily meter data.
    Source: stg_procount__meterdaily
*/

select * from {{ ref('stg_procount__meterdaily') }}
