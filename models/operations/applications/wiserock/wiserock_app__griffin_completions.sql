{{
    config(
        materialized='table',
        tags=['wiserock', 'share', 'procount']
    )
}}

-- WiseRock share: Completion master data
select * from {{ ref('stg_procount__completions') }}

