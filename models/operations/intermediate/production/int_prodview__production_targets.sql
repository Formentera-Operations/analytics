{{
    config(
        enable=true,
        materialized='view'
    )
}}

WITH targetdaily AS (
    SELECT*
    FROM {{ ref('stg_prodview__production_targets_daily') }}
),

parenttarget AS (
    SELECT*
    FROM {{ ref('stg_prodview__production_targets') }}
),

header AS (
    SELECT*
    FROM {{ ref('int_prodview__well_header') }}
),

source AS (
    SELECT
         p."CC Forecast Name"
        ,t."Created At (UTC)"
        ,t."Created By"
        ,t."Flow Net ID"
        ,p."Is Use in Diff from Target Calculations"
        ,t."Last Mod At (UTC)"
        ,t."Last Mod By"
        ,t."Target Daily Date" as "Prod Date"
        --,t."Target Record ID"
        --,t."Target Daily Record ID"
        ,t."Target Daily Rate Condensate bbl per Day"
        ,t."Target Daily Rate Gas mcf per Day"
        ,t."Target Daily Rate Hcliq bbl per Day"
        ,t."Target Daily Rate Ngl bbl per Day"
        ,t."Target Daily Rate Oil bbl per Day"
        ,t."Target Daily Rate Sand bbl per Day"
        ,t."Target Daily Rate Water bbl per Day"
        ,t."Target Daily Record ID"
        ,p."Target Record ID"
        ,p."Target Start Date"        
        ,p."Target Type"
        ,h."Unit Record ID"
    FROM targetdaily t
    LEFT JOIN parenttarget p
    ON p."Target Record ID" = t."Target Record ID"
    LEFT JOIN header h 
    on p."Parent Target Record ID" = h."Completion Record ID"
       --where not i."Target Record ID" is null
 
)

select  *
 from source
--order by "Prod Date" Desc
