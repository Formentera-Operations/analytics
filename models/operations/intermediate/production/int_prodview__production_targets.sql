{{
    config(
        enable=true,
        materialized='view'
    )
}}

WITH targetdaily AS (
    SELECT*
    FROM {{ ref('stg_prodview_production_targets_daily') }}
),

targetintegration AS (
    SELECT*
    FROM {{ ref('stg_prodview_production_targets') }}
),

source AS (
    SELECT
         i."CC Forecast Name"
        ,t."Created At (UTC)"
        ,t."Created By"
        ,t."Flow Net ID"
        ,i."Is Use in Diff from Target Calculations"
        ,t."Last Mod At (UTC)"
        ,t."Last Mod By"
        ,t."Target Daily Date"
        --,t."Target Record ID"
        ,t."Target Daily Record ID"
        ,t."Target Daily Rate Condensate bbl per Day"
        ,t."Target Daily Rate Gas mcf per Day"
        ,t."Target Daily Rate Hcliq bbl per Day"
        ,t."Target Daily Rate Ngl bbl per Day"
        ,t."Target Daily Rate Oil bbl per Day"
        ,t."Target Daily Rate Sand bbl per Day"
        ,t."Target Daily Rate Water bbl per Day"
        ,i."Unit Record ID"
        ,i."Target Record ID"
        ,i."Target Start Date"
        ,i."Target Type"
    FROM targetdaily t
    LEFT JOIN targetintegration i
    ON t."Target Record ID" = i."Target Record ID"
       where not i."Target Record ID" is null
 
)

select * from source
order by "Target Daily Date" Desc
