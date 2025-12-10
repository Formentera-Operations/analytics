{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin cross reference data.
    Source: stg_procount__cross_reference
*/

select * from {{ ref('stg_procount__cross_reference') }}
