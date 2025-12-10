{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin daily tank data.
    Source: stg_procount__tankdaily
*/

select * from {{ ref('stg_procount__tankdaily') }}
