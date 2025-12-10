{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin meter master data.
    Source: stg_procount__meters
*/

select * from {{ ref('stg_procount__meters') }}
