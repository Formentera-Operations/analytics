{{
    config(
        materialized='table',
        tags=['wiserock_app', 'griffin', 'procount']
    )
}}

/*
    WiseRock App sharing model for Griffin well test data.
    Source: stg_procount__completionwelltest
*/

select * from {{ ref('stg_procount__completionwelltest') }}
