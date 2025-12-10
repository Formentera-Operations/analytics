{{
    config(
        materialized='table',
        tags=['wiserock', 'share', 'procount']
    )
}}

-- WiseRock share: Monthly completion disposition
select * from {{ ref('stg_procount__completionmonthlydisp') }}
