{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}



WITH prodvolume as (
    Select
        *
    FROM {{ ref('int_prodview__production_volumes') }}
        where "Prod Date" > '2021-12-31' and "Prod Date" < CAST(GETDATE() AS date) - 1
)

,bounds AS (
  SELECT 
    "Unit Record ID"
    ,"Status Record ID"
    ,TO_DATE(MIN("Prod Date")) AS start_date
    ,TO_DATE(MAX("Prod Date")) AS end_date
    ,(DATEDIFF(DAY, MIN("Prod Date"), MAX("Prod Date")) + 30) AS num_days
  FROM prodvolume
  GROUP BY 
    "Unit Record ID"
    ,"Status Record ID"
    ,"Prod Status"
  order by "Unit Record ID" desc
),

  -- One row per calendar day from 2022-01-01 to today
 datesql as(
    SELECT 
        SEQ4() AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 2000))  -- big constant upper bound
)

,proddatefill as (
    SELECT 
        b."Unit Record ID" as "Unit Record ID Fill"
        ,b."Status Record ID" as "Status Record ID Fill"
        ,b.start_date
        ,b.end_date
        ,DATEADD(day, n, b.start_date) AS "Prod Date Fill"
    FROM bounds b
    JOIN datesql 
        ON n <= b.num_days -- dynamic limit applied here
        order by "Unit Record ID Fill", "Prod Date Fill" Desc
)

,prodjoin as (
    SELECT
        p.*
        ,f.*
    FROM proddatefill f
    LEFT JOIN prodvolume p
        ON f."Unit Record ID Fill" = p."Unit Record ID" AND 
        coalesce(f."Status Record ID Fill", 'blank') = coalesce(p."Status Record ID", 'blank') AND
        f."Prod Date Fill" = P."Prod Date"
)
,totalboe as (
    SELECT
        *
        ,(coalesce("Gross Allocated WH Oil", 0) + (coalesce("Gross Allocated WH New Gas", 0)/6)) AS "Total BOE"
    FROM prodjoin
    order by "Unit Record ID Fill", "Prod Date Fill"

)

,totalboerange as (
    SELECT
    *
  -- Rolling sum of the PRIOR 7 calendar days (excludes current day)
  ,CAST(sum("Total BOE")  OVER (PARTITION BY "Unit Record ID Fill"
    ORDER BY "Prod Date"
    ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
  ) AS DECIMAL(38,3)) AS "Total BOE last 7 Days"
    -- Rolling sum of the PRIOR 30 calendar days (excludes current day)
  ,CAST(sum("Total BOE")  OVER (PARTITION BY "Unit Record ID Fill"
    ORDER BY "Prod Date"
    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
  ) AS DECIMAL(38,3)) AS "Total BOE last 30 Days"
FROM totalboe
ORDER BY "Unit Record ID", "Prod Date Fill" desc
)

Select 
    *
    ,CASE WHEN "Total BOE last 7 Days" < 1 THEN false ELSE true END AS "Has Prod Last 7 Days"
    ,CASE WHEN "Total BOE last 30 Days" < 1 THEN false ELSE true END AS "Has Prod Last 30 Days"
from totalboerange
where "Prod Date" > '2022-12-31' and "Prod Date" < CAST(GETDATE() AS date) - 1
ORDER BY "Unit Record ID", "Prod Date Fill" desc



