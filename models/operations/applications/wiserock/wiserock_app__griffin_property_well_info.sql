{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin property/well information data.
    Source: stg_procount__property_well_info
*/

select * from {{ ref('stg_procount__property_well_info') }}
