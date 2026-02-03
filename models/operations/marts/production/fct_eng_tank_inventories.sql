{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}



    Select
        *
    FROM {{ ref('int_prodview__tank_volumes') }}
        where "Date" > '2021-12-31'
        and not "Current Facility ID" is null
