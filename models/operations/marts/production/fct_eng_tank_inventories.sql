{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}



    Select
        *
    FROM {{ ref('int_prodview__tank_volumes') }}
        where "Date" > LAST_DAY(DATEADD(year, -3,CURRENT_DATE()), year)
        and not "Current Facility ID" is null
