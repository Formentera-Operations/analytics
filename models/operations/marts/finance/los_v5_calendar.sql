{{ config(
    enable= true,
    materialized='table'
) }}

FROM {{ ref('int_oda_calendar') }}
WHERE DATE >= '2020-01-01'