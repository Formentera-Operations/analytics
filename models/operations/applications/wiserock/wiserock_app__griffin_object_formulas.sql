{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin object formulas data.
    Source: stg_procount__object_formulas
*/

select * from {{ ref('stg_procount__object_formulas') }}
