{{
    config(
        enable=true,
        materialized='view'
    )
}}



with downtime as (
    select
        "Completion Downtime Record ID"
        ,"Completion Record ID"
        ,"Type of Downtime Entry"
        ,Product
        ,"Location"
        ,"Is failure"
        ,TO_DATE("First Day") AS "First Day"
        ,COALESCE("Hours Down", 0) AS "Hours Down"
        ,TO_DATE("Last Day") AS "Last Day"
        ,CASE
            WHEN "Total Downtime Hours" IS NULL THEN "Hours Down"
            ELSE COALESCE("Total Downtime Hours", 0) 
        END AS "Total Downtime Hours"
        ,"Downtime Code"
        ,"Downtime Code 2"
        ,"Downtime Code 3"
        ,Comments
    from {{ ref('stg_prodview__completion_downtimes') }}
    where "First Day" > '2021-12-31'
        ORDER BY "Completion Record ID", "First Day"
)

,consecutives as (
  SELECT
      *
    , CASE
        WHEN LOWER("Type of Downtime Entry") = 'single day'
         AND LAG("First Day") OVER (
               PARTITION BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3"
               ORDER BY "First Day"
             ) = DATEADD(day, -1, "First Day")
          THEN 0              -- still consecutive, same island
          ELSE 1              -- break → new island
      END AS break_flag
  FROM downtime
)

,islands AS (
  SELECT
      *
    , SUM(break_flag) OVER (
        PARTITION BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3"
        ORDER BY "First Day"
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS island_id
  FROM consecutives
)

, start_end_dates as (
    SELECT
        *
        ,MIN("Completion Downtime Record ID") OVER (
            PARTITION BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3", island_id
            ) AS "Completion Downtime Event ID"
        ,MIN("First Day") OVER (
            PARTITION BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3", island_id
            ) AS "Downtime First Day"
        ,MAX("Last Day")   OVER (
            PARTITION BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3", island_id
            ) AS "Downtime Last Day"
        ,SUM("Total Downtime Hours") OVER (
            PARTITION BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3", island_id
            ) AS "Total Consecutive Downtime Hours"
    FROM islands
    ORDER BY "Completion Record ID", "Downtime Code", "Downtime Code 2", "Downtime Code 3", "First Day"
)

Select
    c."Completion Downtime Event ID"
    ,c."Completion Downtime Record ID"
    ,c."Completion Record ID"
    ,c."Type of Downtime Entry"
    ,c.Product
    ,c."Location"
    ,c."Is failure"
    ,c."First Day"
    ,c."Hours Down"
    ,c."Last Day"
    ,c."Total Downtime Hours"
    ,c."Total Consecutive Downtime Hours"
    ,c."Downtime First Day"
    ,c."Downtime Last Day"
    ,c."Downtime Code"
    ,c."Downtime Code 2"
    ,c."Downtime Code 3"
    ,c.Comments
    --,h."Asset Company"
    --,h."Route Name"
    --,h."Foreman"
    --,h."Foreman Area"
    --,h."Prodview Well Name"
    ,h."Unit Record ID"
FROM start_end_dates c
LEFT JOIN {{ ref('int_fct_well_header') }} h
ON c."Completion Record ID" = h."Completion Record ID"
--WHERE "Type of Downtime Entry" = 'single day' AND "Consecutive First Day" = true
ORDER BY "Completion Record ID", "First Day" desc