{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

WITH unitroute as (
    Select
        *
    FROM {{ ref('int_dim_route') }}
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
