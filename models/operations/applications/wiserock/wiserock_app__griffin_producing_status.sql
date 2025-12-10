{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin producing status reference data.
    Source: stg_procount__producing_status
*/

select * from {{ ref('stg_procount__producing_status') }}
