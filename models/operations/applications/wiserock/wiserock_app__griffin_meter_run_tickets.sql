{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin meter run ticket data.
    Source: stg_procount__meter_run_tickets
*/

select * from {{ ref('stg_procount__meter_run_tickets') }}
