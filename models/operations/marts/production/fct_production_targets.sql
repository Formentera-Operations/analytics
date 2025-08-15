{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH prodtarget as (
    Select
         "CC Forecast Name"
        --,"Created At (UTC)"
        --,"Created By"
        --,"Flow Net ID"
        --,"Is Use in Diff from Target Calculations"
        --,"Last Mod At (UTC)"
        --,"Last Mod By"
        ,"Target Daily Date" as "Prod Date"
        ,YEAR("Target Daily Date") as "Budget Year"
        --,"Target Daily Record ID"
        --,"Target Daily Rate Condensate bbl per Day"
        ,("Target Daily Rate Hcliq bbl per Day" + ("Target Daily Rate Gas mcf per Day"/6)) as "Budget - Gross BOE"
        ,"Target Daily Rate Gas mcf per Day" as "Budget - Gross Gas"
        ,"Target Daily Rate Hcliq bbl per Day" as "Budget - Gross Oil"
        --,"Target Daily Rate Ngl bbl per Day"
        --,"Target Daily Rate Oil bbl per Day"
        --,"Target Daily Rate Sand bbl per Day"
        ,"Target Daily Rate Water bbl per Day" as "Budget - Gross Water"
        ,"Unit Record ID"
        --,"Target Record ID"
        ,"Target Start Date"
        ,"Target Type"
    FROM {{ ref('int_prodview__production_targets') }}
)

Select 
    *
    ,("Budget - Gross BOE" / 24) as "Budget - Gross BOE/hr"
from prodtarget
where "Prod Date" > '2021-12-31'
and "Target Type" = 'Budget'
order by "Prod Date" Desc