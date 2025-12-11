{{
    config(
        materialized='table',
        tags=['wiserock', 'share', 'procount']
    )
}}

-- WiseRock share: Daily completion production
select * from {{ ref('stg_procount__completiondaily') }}
