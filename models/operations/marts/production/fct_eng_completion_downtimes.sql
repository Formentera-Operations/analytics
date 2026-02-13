{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with downtimes as (
    select *
    from {{ ref('int_prodview__completion_downtimes') }}
)

select *
from downtimes
where first_day > last_day(dateadd(year, -3, current_date()), year)
order by first_day
