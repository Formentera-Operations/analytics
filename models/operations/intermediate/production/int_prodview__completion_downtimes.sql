{{
    config(
        enable=false,
        materialized='view'
    )
}}

WITH downtimes AS (
    SELECT
        *
    FROM {{ ref('stg_prodview__completion_downtimes') }}
)

, groupby AS (
    SELECT
        
)