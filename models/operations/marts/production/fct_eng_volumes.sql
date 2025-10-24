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

,tbl as (
  Select 
      *
      ,CASE WHEN "Total BOE last 7 Days" < 1 THEN false ELSE true END AS "Has Prod Last 7 Days"
      ,CASE WHEN "Total BOE last 30 Days" < 1 THEN false ELSE true END AS "Has Prod Last 30 Days"
  from totalboerange
  where "Prod Date" > '2022-12-31' and "Prod Date" < CAST(GETDATE() AS date) - 1
)

SELECT
  "Allocated Gas Equivalent of HCLiq mcf"
  ,"Allocated Gas mcf"
  ,"Allocated HCLiq bbl"
  ,"Allocated NGL bbl"
  ,"Allocated Oil bbl"
  ,"Allocated Sand bbl"
  ,"Allocated Water bbl"
  ,"Allocation Day of Month"
  ,"Allocation Factor Gas"
  ,"Allocation Factor HCLiq"
  ,"Allocation Factor Sand"
  ,"Allocation Factor Water"
  ,"Allocation Month"
  ,"Allocation Record ID"
  ,"Allocation Year"
  ,"Bottomhole Pressure psi"
  ,"Bottomhole Temperature F"
  ,"Casing Pressure psi"
  ,"Change In Inventory Gas Equivalent Oil Cond mcf"
  ,"Change In Inventory Oil Condensate bbl"
  ,"Change In Inventory Sand bbl"
  ,"Change In Inventory Water bbl"
  ,"Choke Size 64ths"
  ,"Closing Inventory Gas Equiv Oil Condensate mcf"
  ,"Closing Inventory Sand bbl"
  ,"Closing Inventory Water bbl"
  ,"Created At (UTC)"
  ,"Created By"
  ,"Cumulated Condensate bbl"
  ,"Cumulated Gas mcf"
  ,"Cumulated Hcliq bbl"
  ,"Cumulated Ngl bbl"
  ,"Cumulated Oil bbl"
  ,"Cumulated Sand bbl"
  ,"Cumulated Water bbl"
  ,"Deferred Gas Production mcf"
  ,"Deferred Oil Condensate Production bbl"
  ,"Deferred Sand Production bbl"
  ,"Deferred Water Production bbl"
  ,"Difference From Target Condensate bbl"
  ,"Difference From Target Gas mcf"
  ,"Difference From Target Hcliq bbl"
  ,"Difference From Target Ngl bbl"
  ,"Difference From Target Oil bbl"
  ,"Difference From Target Sand bbl"
  ,"Difference From Target Water bbl"
  ,"Disposed Allocated Flare Gas mcf"
  ,"Disposed Allocated Fuel Gas mcf"
  ,"Disposed Allocated Incineration Gas mcf"
  ,"Disposed Allocated Injected Gas mcf"
  ,"Disposed Allocated Injected Water bbl"
  ,"Disposed Allocated Sales Condensate bbl"
  ,"Disposed Allocated Sales Ngl bbl"
  ,"Disposed Allocated Sales Oil bbl"
  ,"Disposed Allocated Vent Gas mcf"
  ,"Down Hours"
  ,"Downtime Code"
  ,"Downtime Code 2"
  ,"Downtime Code 3"
  ,"Downtime Last Date"
  ,"Downtime Record ID"
  ,"Downtime Start Date"
  ,"Dynamic Viscosity Pascal Seconds"
  ,"Gathered Gas mcf"
  ,"Gathered HCLiq bbl"
  ,"Gathered Sand bbl"
  ,"Gathered Water bbl"
  ,"Gross Allocated BOE"
  ,"Gross Allocated Sales Gas"
  ,"Gross Allocated Sales Oil"
  ,"Gross Allocated WH New Gas"
  ,"Gross Allocated WH Oil"
  ,"Gross Downtime BOE"
  ,"H2s Daily Reading ppm"
  ,"Has Prod Last 30 Days"
  ,"Has Prod Last 7 Days"
  ,"Injected Lift Gas bbl"
  ,"Injected Load Oil Condensate bbl"
  ,"Injected Load Water bbl"
  ,"Injected Sand bbl"
  ,"Injection Pressure psi"
  ,"Injection Well Gas mcf"
  ,"Injection Well Oil Cond bbl"
  ,"Injection Well Sand bbl"
  ,"Injection Well Water bbl"
  ,"Kinematic Viscosity In2 Per S"
  ,"Last Completion Parameter Record ID"
  ,"Last Mod At (UTC)"
  ,"Last Mod By"
  ,"Last Pump Entry Record ID"
  ,"Last Pump Entry Table"
  ,"Last Test Record ID"
  ,"Line Pressure psi"
  ,"Net 2-Stream Sales BOE"
  ,"Net Gas Sales"
  ,"Net Oil Prod"
  ,"Net Revenue Interest Gas pct"
  ,"Net Revenue Interest Oil Cond pct"
  ,"Net Revenue Interest Sand pct"
  ,"Net Revenue Interest Water pct"
  ,"New Production Condensate bbl"
  ,"New Production Hcliq Gas Equivalent mcf"
  ,"New Production Ngl bbl"
  ,"New Production Oil bbl"
  ,"New Production Sand bbl"
  ,"New Production Water bbl"
  ,"Opening Inventory Gas Equivalent Oil Cond mcf"
  ,"Opening Inventory Oil Condensate bbl"
  ,"Opening Inventory Sand bbl"
  ,"Opening Inventory Water bbl"
  ,"Operating Time Hours"
  ,"PH Level"
  ,"Prod Date"
  ,"Prod Status"
  ,"Pump Efficiency pct"
  ,"Recovered Lift Gas mcf"
  ,"Recovered Load Oil Condensate bbl"
  ,"Recovered Load Water bbl"
  ,"Recovered Sand bbl"
  ,"Remaining Lift Gas mcf"
  ,"Remaining Load Oil Condensate bbl"
  ,"Remaining Load Water bbl"
  ,"Remaining Sand bbl"
  ,"Reporting Facility Record ID"
  ,"Shut In Casing Pressure psi"
  ,"Shut In Tubing Pressure psi"
  ,"Starting Lift Gas mcf"
  ,"Starting Load Oil Condensate bbl"
  ,"Starting Load Water bbl"
  ,"Starting Sand bbl"
  ,"END_DATE" as "Status End Date"
  ,"Status Record ID"
  ,"START_DATE" as "Status Start Date"
  ,"Tank Oil INV."
  ,"Total BOE"
  ,"Total BOE last 30 Days"
  ,"Total BOE last 7 Days"
  ,"Tubing Pressure psi"
  ,"Unit Record ID"
  ,"Volume Lost Target Gas"
  ,"Volume Lost Target hcliq"
  ,"Wellhead Pressure psi"
  ,"Wellhead Temperature F"
  ,"Working Interest Gas pct"
  ,"Working Interest Oil Cond pct"
  ,"Working Interest Sand pct"
  ,"Working Interest Water pct"
FROM tbl
ORDER BY "Unit Record ID", "Prod Date" desc



