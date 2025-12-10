{{
    config(
        tags=['wiserock', 'share', 'procount']
    )
}}

-- WiseRock share: Daily completion disposition
select * from {{ ref('stg_procount__completiondailydisp') }}
