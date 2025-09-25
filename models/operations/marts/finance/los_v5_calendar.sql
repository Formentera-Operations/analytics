{{ config(
    enable= true,
    materialized='table'
) }}

select *
FROM {{ ref('int_oda_calendar') }}
WHERE DATE >= '2020-01-01'