{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin monthly meter data.
    Source: stg_procount__metermonthly
*/

select * from {{ ref('stg_procount__metermonthly') }}
