{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin tank run ticket data.
    Source: stg_procount__tank_run_tickets
*/

select * from {{ ref('stg_procount__tank_run_tickets') }}
