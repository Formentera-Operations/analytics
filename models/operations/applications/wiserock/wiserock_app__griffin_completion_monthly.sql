{{
    config(
        materialized='table',
        tags=['wiserock', 'share', 'procount']
    )
}}

-- WiseRock share: Monthly completion production
select * from {{ ref('stg_procount__completionmonthly') }}
