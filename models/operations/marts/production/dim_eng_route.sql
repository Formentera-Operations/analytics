{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'dim']
) }}

WITH unitroute as (
    Select
        *
    FROM {{ ref('int_dim_route') }}
),

unit as (
    select distinct
    "Route Record ID"
    from {{ ref('int_prodview__well_header') }}
)

Select
    r."Backup Lease Operator"
    ,r."Created By"
    ,r."Created Date (UTC)"
    ,r."Flow Net ID"
    ,r."Foreman"
    ,r."Last Mod By"
    ,r."Last Mod Date (UTC)"
    ,r."NOTES"
    ,r."Primary Lease Operator"
    ,r."Route Name" as "Route"
    ,r."Route Name Clean" as "Route Name"
    ,r."Route Parent Record ID"
    ,r."Route Record ID"
FROM unitroute r 
    LEFT JOIN unit u 
    ON u."Route Record ID"= r."Route Record ID"
