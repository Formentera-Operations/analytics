{{
    config(
        materialized='view'
    )
}}

WITH unitalloc AS (
    SELECT*
    FROM {{ ref('stg_prodview__daily_allocations') }}
),

compdowntime AS (
    SELECT*
    FROM {{ ref('stg_prodview__completion_downtimes') }}
),

compparam AS (
    SELECT *
    FROM {{ ref('stg_prodview__completion_parameters') }}
),

unitstatus AS (
    SELECT*
    FROM {{ ref('stg_prodview__status') }}
),

prod AS (
    SELECT
        a."Allocated Gas Equivalent of HCLiq mcf"
        ,a."Allocated Gas mcf"
        ,a."Allocated HCLiq bbl"
        ,a."Allocated NGL bbl"
        ,a."Allocated Oil bbl"
        ,a."Allocated Sand bbl"
        ,a."Allocated Water bbl"
        ,a."Allocation Day of Month"
        ,a."Allocation Factor Gas"
        ,a."Allocation Factor HCLiq"
        ,a."Allocation Factor Sand"
        ,a."Allocation Factor Water"
        ,a."Allocation Month"
        ,a."Allocation Record ID"
        ,a."Allocation Year"
        ,p."Bottomhole Pressure psi"
        ,p."Bottomhole Temperature F"
        ,p."Casing Pressure psi"
        ,a."Change In Inventory Gas Equivalent Oil Cond mcf"
        ,a."Change In Inventory Oil Condensate bbl"
        ,a."Change In Inventory Sand bbl"
        ,a."Change In Inventory Water bbl"
        ,p."Choke Size 64ths"
        ,a."Closing Inventory Gas Equiv Oil Condensate mcf"
        ,a."Closing Inventory Oil Condensate bbl" as "Tank Oil INV."
        ,a."Closing Inventory Sand bbl"
        ,a."Closing Inventory Water bbl"
        ,a."Created At (UTC)"
        ,a."Created By"
        ,a."Cumulated Condensate bbl"
        ,a."Cumulated Gas mcf"
        ,a."Cumulated Hcliq bbl"
        ,a."Cumulated Ngl bbl"
        ,a."Cumulated Oil bbl"
        ,a."Cumulated Sand bbl"
        ,a."Cumulated Water bbl"
        ,a."Deferred Gas Production mcf"
        ,a."Deferred Oil Condensate Production bbl"
        ,a."Deferred Sand Production bbl"
        ,a."Deferred Water Production bbl"
        ,a."Difference From Target Condensate bbl"
        ,a."Difference From Target Gas mcf"
        ,a."Difference From Target Hcliq bbl"
        ,a."Difference From Target Ngl bbl"
        ,a."Difference From Target Oil bbl"
        ,a."Difference From Target Sand bbl"
        ,a."Difference From Target Water bbl"
        ,a."Disposed Allocated Flare Gas mcf"
        ,a."Disposed Allocated Fuel Gas mcf"
        ,a."Disposed Allocated Incineration Gas mcf"
        ,a."Disposed Allocated Injected Gas mcf"
        ,a."Disposed Allocated Injected Water bbl"
        ,a."Disposed Allocated Sales Condensate bbl"
        ,a."Disposed Allocated Sales Gas mcf" as "Gross Allocated Sales Gas"
        ,a."Disposed Allocated Sales Hcliq bbl" as "Gross Allocated Sales Oil"
        ,a."Disposed Allocated Sales Ngl bbl"
        ,a."Disposed Allocated Sales Oil bbl"
        ,a."Disposed Allocated Vent Gas mcf"
        ,a."Downtime Hours" as "Down Hours"
        ,d."Downtime Code 2"
        ,d."Downtime Code 3"
        ,d."Downtime Code"
        ,a."Downtime Record ID"
        ,p."Dynamic Viscosity Pascal Seconds"
        ,a."Gathered Gas mcf"
        ,a."Gathered HCLiq bbl"
        ,a."Gathered Sand bbl"
        ,a."Gathered Water bbl"
        ,COALESCE(a."New Production HCLiq bbl", 0 ) + (COALESCE(a."New Production Gas mcf", 0)/ 6) as "Gross Allocated BOE"
        ,p."H2s Daily Reading ppm"
        ,a."Injected Lift Gas bbl"
        ,a."Injected Load Oil Condensate bbl"
        ,a."Injected Load Water bbl"
        ,a."Injected Sand bbl"
        ,p."Injection Pressure psi"
        ,a."Injection Well Gas mcf"
        ,a."Injection Well Oil Cond bbl"
        ,a."Injection Well Sand bbl"
        ,a."Injection Well Water bbl"
        ,p."Kinematic Viscosity In2 Per S"
        ,GREATEST(
            NVL(a."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(d."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(p."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ,NVL(s."Last Mod At (UTC)", TO_TIMESTAMP_TZ('0000-01-01T00:00:00.000Z'))
            ) AS "Last Mod At (UTC)"
        ,a."Last Mod By"
        ,a."Last Completion Parameter Record ID"
        ,a."Last Pump Entry Record ID"
        ,a."Last Pump Entry Table"
        ,a."Last Test Record ID"
        ,p."Line Pressure psi"
        ,(a."Disposed Allocated Sales Gas mcf" * a."Net Revenue Interest Gas pct") / 100 as "Net Gas Sales"
        ,(a."New Production HCLiq bbl" * a."Net Revenue Interest Oil Cond pct") / 100 as "Net Oil Prod"
        ,a."Net Revenue Interest Gas pct"
        ,a."Net Revenue Interest Oil Cond pct"
        ,a."Net Revenue Interest Sand pct"
        ,a."Net Revenue Interest Water pct"
        ,a."New Production Condensate bbl"
        ,COALESCE(a."New Production Gas mcf",0) as "Gross Allocated WH New Gas"
        ,COALESCE(a."New Production HCLiq bbl",0) as "Gross Allocated WH Oil"
        ,a."New Production Hcliq Gas Equivalent mcf"
        ,a."New Production Ngl bbl"
        ,a."New Production Oil bbl"
        ,a."New Production Sand bbl"
        ,a."New Production Water bbl"
        ,a."Opening Inventory Gas Equivalent Oil Cond mcf"
        ,a."Opening Inventory Oil Condensate bbl"
        ,a."Opening Inventory Sand bbl"
        ,a."Opening Inventory Water bbl"
        ,a."Operating Time Hours"
        ,p."PH Level"
        ,a."Allocation Date" as "Prod Date"
        ,s."Status Record ID"
        ,s."Status" as "Prod Status"
        ,a."Pump Efficiency pct"
        ,a."Recovered Lift Gas mcf"
        ,a."Recovered Load Oil Condensate bbl"
        ,a."Recovered Load Water bbl"
        ,a."Recovered Sand bbl"
        ,a."Remaining Lift Gas mcf"
        ,a."Remaining Load Oil Condensate bbl"
        ,a."Remaining Load Water bbl"
        ,a."Remaining Sand bbl"
        ,a."Reporting Facility Record ID"
        ,p."Shut In Casing Pressure psi"
        ,p."Shut In Tubing Pressure psi"
        ,a."Starting Lift Gas mcf"
        ,a."Starting Load Oil Condensate bbl"
        ,a."Starting Load Water bbl"
        ,a."Starting Sand bbl"
       -- ,a."Status Record ID"
        ,p."Tubing Pressure psi"
        ,a."Unit Record ID"
        ,((COALESCE(a."New Production Gas mcf", 0) - COALESCE(a."Difference From Target Gas mcf", 0))
            /24)
            * COALESCE(a."Downtime Hours", 0)
        as "Volume Lost Target Gas"
        ,((COALESCE(a."New Production HCLiq bbl", 0 ) - COALESCE(a."Difference From Target Hcliq bbl", 0))
                /24)
            * COALESCE(a."Downtime Hours", 0)
        as "Volume Lost Target hcliq"
        ,p."Wellhead Pressure psi"
        ,p."Wellhead Temperature F"
        ,a."Working Interest Gas pct"
        ,a."Working Interest Oil Cond pct"
        ,a."Working Interest Sand pct"
        ,a."Working Interest Water pct"
    FROM  unitalloc a
    LEFT JOIN  compdowntime d 
        ON a."Downtime Record ID" = d."Completion Downtime Record ID"
    LEFT JOIN compparam p 
        ON a."Last Completion Parameter Record ID" = p."Completion Parameter Record ID"
    LEFT JOIN unitstatus s 
        ON a."Status Record ID" = s."Status Record ID"
)

SELECT 
    *
    ,COALESCE("Net Oil Prod", 0) + (COALESCE("Net Gas Sales", 0)/6) as "Net 2-Stream Sales BOE"
    ,(COALESCE("Volume Lost Target hcliq", 0) + (COALESCE("Volume Lost Target Gas", 0)/ 6))*-1 as "Gross Downtime BOE"
FROM prod