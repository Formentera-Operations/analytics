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
       r.*
    FROM unitroute r 
        LEFT JOIN unit u 
        ON u."Route Record ID"= r."Route Record ID"
