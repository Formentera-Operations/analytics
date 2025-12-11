{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin equipment master data.
    Source: stg_procount__equipment
*/

select * from {{ ref('stg_procount__equipment') }}
