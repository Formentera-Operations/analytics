{{
    config(
        materialized='table',
        tags=['wiserock', 'share', 'procount']
    )
}}

-- WiseRock share: Downtime records
select * from {{ ref('stg_procount__allocation_types') }}