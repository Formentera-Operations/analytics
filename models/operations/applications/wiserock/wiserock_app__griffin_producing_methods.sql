{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin producing methods reference data.
    Source: stg_procount__producing_methods
*/

select * from {{ ref('stg_procount__producing_methods') }}
