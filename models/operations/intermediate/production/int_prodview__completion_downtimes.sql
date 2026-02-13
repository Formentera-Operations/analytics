{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with downtime as (
    select
        id_rec,
        id_rec_parent,
        downtime_type,
        product,
        location,
        is_failure,
        first_day::date as first_day,
        last_day::date as last_day,
        downtime_code_1,
        downtime_code_2,
        downtime_code_3,
        comments,
        coalesce(hours_down, 0) as hours_down,
        case
            when total_downtime_hours is null then hours_down
            else coalesce(total_downtime_hours, 0)
        end as total_downtime_hours
    from {{ ref('stg_prodview__completion_downtimes') }}
    where first_day > '2021-12-31'
    order by id_rec_parent, first_day
),

consecutives as (
    select
        *,
        case
            when
                lower(downtime_type) = 'single day'
                and lag(first_day) over (
                    partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3
                    order by first_day
                ) = dateadd(day, -1, first_day)
                then 0
            else 1
        end as break_flag
    from downtime
),

islands as (
    select
        *,
        sum(break_flag) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3
            order by first_day
            rows between unbounded preceding and current row
        ) as island_id
    from consecutives
),

start_end_dates as (
    select
        *,
        min(id_rec) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as downtime_event_id,
        min(first_day) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as downtime_first_day,
        max(last_day) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as downtime_last_day,
        sum(total_downtime_hours) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as total_consecutive_downtime_hours
    from islands
    order by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, first_day
)

select
    c.downtime_event_id,
    c.id_rec,
    c.id_rec_parent,
    c.downtime_type,
    c.product,
    c.location,
    c.is_failure,
    c.first_day,
    c.hours_down,
    c.last_day,
    c.total_downtime_hours,
    c.total_consecutive_downtime_hours,
    c.downtime_first_day,
    c.downtime_last_day,
    c.downtime_code_1,
    c.downtime_code_2,
    c.downtime_code_3,
    c.comments,
    h."Unit Record ID" as unit_record_id,
    datediff(day, c.downtime_first_day, coalesce(c.downtime_last_day, current_date() - 1)) as days_down
from start_end_dates c
left join {{ ref('int_fct_well_header') }} h
    on c.id_rec_parent = h."Completion Record ID"
order by c.id_rec_parent asc, c.first_day desc
