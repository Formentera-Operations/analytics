{{ config(
    enable= true,
    materialized='view'
) }}

WITH prodstatus as (
    Select
    *
      ,CASE 
        WHEN "Status" = 'Active' THEN 'Producing'
        WHEN "Status" = 'Completing' THEN 'Producing'
        WHEN "Status" = 'ESP' THEN 'Producing'
        WHEN "Status" = 'ESP - OWNED' THEN 'Producing'
        WHEN "Status" = 'FLOWING' THEN 'Producing'
        WHEN "Status" = 'Flowing' THEN 'Producing'
        WHEN "Status" = 'FLOWING - CASING' THEN 'Producing'
        WHEN "Status" = 'FLOWING - TUBING' THEN 'Producing'
        WHEN "Status" = 'GAS LIFT' THEN 'Producing'
        WHEN "Status" = 'INACTIVE' THEN 'Shut In'
        WHEN "Status" = 'INACTIVE COMPLETED' THEN 'Shut In'
        WHEN "Status" = 'INACTIVE INJECTOR' THEN 'Shut In'
        WHEN "Status" = 'INACTIVE PRODUCER' THEN 'Shut In'
        WHEN "Status" = 'INJECTING' THEN 'Injecting'
        WHEN "Status" = 'Producer' THEN 'Producing'
        WHEN "Status" = 'SHUT IN' THEN 'Shut In'
        WHEN "Status" = 'Shut-In' THEN 'Shut In'
    ELSE "Status" END 
        AS "Status Clean"
    FROM {{ ref('stg_prodview__status') }}  --, "Status Record ID"
)
/*,

allocation as (
    select distinct
    "Status Record ID"
    from {{ ref('int_prodview__production_volumes') }}
    
)
*/
Select
       s.*
    FROM prodstatus s 
 --       LEFT JOIN allocation a 
 --       ON a."Status Record ID" = s."Status Record ID"
