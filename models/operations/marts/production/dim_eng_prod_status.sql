{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'dim']
) }}

WITH prodstatus as (
    Select
    *
    FROM {{ ref('int_dim_prod_status') }} --, "Status Record ID"
),

allocation as (
    select distinct
    "Status Record ID"
    from {{ ref('int_prodview__production_volumes') }}
    
)
Select
    s."Calculate Lost Production"
    ,s."Comment"
    ,s."Commingled"
    ,s."Completion Type"
    ,s."Created At (UTC)"
    ,s."Created By"
    ,s."Flow Direction"
    ,s."Flow Net ID"
    ,s."Include In Well Count"
    ,s."Last Mod At (UTC)"
    ,s."Last Mod By"
    ,s."Oil Or Condensate"
    ,s."Primary Fluid Type"
    ,s."Status Clean" as "Prod Status"
    ,s."Production Method"
    ,s."Status Date"
    ,s."Status Parent Record ID"
    ,s."Status"
    ,s."Status Record ID"
FROM prodstatus s 
    LEFT JOIN allocation a 
    ON a."Status Record ID" = s."Status Record ID"
