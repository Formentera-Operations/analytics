{{
    config(
        materialized='ephemeral'
    )
}}

{#
    ARCHITECTURAL NOTE:
    This intermediate aggregates three gold-layer facts directly:
      - fct_daily_drilling_cost (incremental, merge on cost_line_id)
      - fct_drilling_time (full-rebuild table, cluster_by=['job_id', 'report_date'])
      - fct_npt_events (full-rebuild table, ~14K rows)

    These facts have different refresh strategies. Do NOT attempt to make this model
    or plat_well__performance_scorecard incremental without reviewing the mixed refresh
    dependencies of the upstream facts.

    If any of these facts changes grain, re-validate this model's SUM logic.
    The unique(eid) test on plat_well__performance_scorecard will catch fan-out.
#}

with drilling_cost as (
    select
        eid,
        count(distinct job_id) as drilling_job_count,
        sum(field_estimate_cost) as total_dc_cost
    from {{ ref('fct_daily_drilling_cost') }}
    where eid is not null
    group by eid
),

drilling_time as (
    select
        eid,
        sum(duration_hours) as total_drilling_hours
    from {{ ref('fct_drilling_time') }}
    where eid is not null
    group by eid
),

npt_events as (
    select
        eid,
        sum(problem_duration_gross_hours) as total_npt_hours
    from {{ ref('fct_npt_events') }}
    where
        eid is not null
        and is_countable_npt
    group by eid
),

final as (
    select
        -- COALESCE on eid ensures wells with time/npt data but no cost records are retained
        coalesce(dc.eid, dt.eid) as eid,

        -- Job count from cost fact (invoicing-driven, most complete source)
        coalesce(dc.drilling_job_count, 0) as drilling_job_count,
        coalesce(dc.total_dc_cost, 0) as total_dc_cost,

        -- Hours from time log fact (operational data, may cover different job set)
        coalesce(dt.total_drilling_hours, 0) as total_drilling_hours,
        coalesce(n.total_npt_hours, 0) as total_npt_hours,

        -- NPT rate capped at 100% — overlapping NPT events can sum above total clock time
        least(
            coalesce(n.total_npt_hours, 0) / nullif(coalesce(dt.total_drilling_hours, 0), 0),
            1.0
        ) as npt_pct,

        -- Data quality flag: raw NPT exceeds total drilling hours (inspect manually)
        coalesce(n.total_npt_hours, 0) > coalesce(dt.total_drilling_hours, 0) as is_npt_anomaly

    from drilling_cost dc
    full outer join drilling_time dt on dc.eid = dt.eid
    left join npt_events n on coalesce(dc.eid, dt.eid) = n.eid
)

select * from final
