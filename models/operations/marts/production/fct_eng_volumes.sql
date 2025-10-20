{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH prodvolume as (
    Select
        *
        ,("Gross Allocated WH New Gas" + "Gross Allocated WH Oil") AS "Total BOE"
    FROM {{ ref('int_prodview__production_volumes') }}
),

totalboe as (
    SELECT
    *
  -- Rolling sum of the PRIOR 7 calendar days (excludes current day)
  ,sum("Total BOE") OVER (
    ORDER BY "Prod Date"
    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
  ) AS "Total BOE last 7 Days"
    -- Rolling sum of the PRIOR 30 calendar days (excludes current day)
  ,sum("Total BOE") OVER (
    ORDER BY "Prod Date"
    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
  ) AS "Total BOE last 30 Days"
FROM prodvolume
    where "Prod Date" > '2021-12-31' and "Prod Date" < CAST(GETDATE() AS date) - 1
)

Select 
    *
    ,CASE WHEN "Total BOE last 7 Days" = 0 THEN false ELSE true END AS "Has Prod Last 7 Days"
    ,CASE WHEN "Total BOE last 30 Days" = 0 THEN false ELSE true END AS "Has Prod Last 30 Days"
from totalboe
ORDER BY "Unit Record ID", "Prod Date" desc