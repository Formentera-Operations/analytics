{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with downtimes as (
    select * from {{ ref('int_prodview__completion_downtimes') }}
)

select
    d.downtime_event_id as "Completion Downtime Event ID",
    d.id_rec as "Completion Downtime Record ID",
    d.id_rec_parent as "Completion Record ID",
    d.downtime_type as "Type of Downtime Entry",
    d.product as "Product",
    d.location as "Location",
    d.is_failure as "Is failure",
    d.first_day as "First Day",
    d.hours_down as "Hours Down",
    d.last_day as "Last Day",
    d.total_downtime_hours as "Total Downtime Hours",
    d.total_consecutive_downtime_hours as "Total Consecutive Downtime Hours",
    d.downtime_first_day as "Downtime First Day",
    d.downtime_last_day as "Downtime Last Day",
    d.downtime_code_1 as "Downtime Code",
    d.downtime_code_2 as "Downtime Code 2",
    d.downtime_code_3 as "Downtime Code 3",
    d.comments as "Comments",
    d.unit_record_id as "Unit Record ID",
    d.days_down as "Days Down"
from downtimes d
where d.first_day > last_day(dateadd(year, -3, current_date()), year)
order by d.first_day
