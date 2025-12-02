{{ config(
    enabled=true,
    materialized='table'
) }}

select *
FROM {{ ref('int_oda_calendar') }}
WHERE "Date" >= '2020-01-01'