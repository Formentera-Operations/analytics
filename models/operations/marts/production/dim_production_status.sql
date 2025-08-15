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
       s.*
    FROM prodstatus s 
        LEFT JOIN allocation a 
        ON a."Status Record ID" = s."Status Record ID"
