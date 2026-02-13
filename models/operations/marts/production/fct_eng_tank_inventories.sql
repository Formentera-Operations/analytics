{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}



select *
from {{ ref('int_prodview__tank_volumes') }}
where
    tank_date > last_day(dateadd(year, -3, current_date()), year)
    and current_facility_id is not null
