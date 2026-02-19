{{
    config(
        materialized='table',
        unique_key='eid',
        tags=['platinum', 'fo', 'well_360'],
        post_hook=[
            "alter table {{ this }} add search optimization on equality(eid, api_10, basin_name, activity_status)"
        ]
    )
}}

{#
    Platinum OBT: Well Performance Scorecard
    =========================================

    POST-HOOK NOTE: The 'add search optimization' post_hook is idempotent here because
    materialized='table' drops and recreates the table on each run, so search optimization
    is always added to a fresh table. If this model is ever changed to incremental,
    the post_hook would need an idempotency guard. (Mirrors same pattern in well_360.sql.)


    GRAIN: One row per EID (well). All known wells in well_360, including
    pre-production and inactive wells.

    PURPOSE:
    Fully denormalized lifetime performance scorecard for BI consumption.
    No joins required at query time. Combines production, financials, drilling,
    and completion data into a single eid-grain table.

    DATA FRESHNESS:
    Drilling EID resolution follows a three-hop chain:
      well_360 (rebuilt daily) → fct_drilling_time / fct_npt_events / fct_daily_drilling_cost
      → int_well_perf__drilling_summary → this model

    UNRESOLVED PRODUCTION:
    ~18.5% of fct_well_production_monthly unit-months have is_eid_unresolved = true
    (eid IS NULL). These are ProdView units without a well_360 match and are
    intentionally excluded from production aggregates here.
#}

with

well_spine as (
    select
        eid,
        oda_well_id,
        api_10,
        api_14,
        well_name,
        cost_center_number,
        company_name,
        operator_name,
        is_operated,
        op_ref_effective as op_ref,
        basin_name,
        state,
        county,
        -- Alias to avoid collision with stim intermediate's stim_lateral_length_ft
        lateral_length_ft as well_lateral_length_ft,
        unified_status as wellbore_status,
        activity_status,
        spud_date,
        first_production_date,
        well_type_oda,
        well_configuration_type,
        is_revenue_generating,
        is_hold_billing,
        is_suspend_revenue,
        is_well,
        operating_group_name,
        cost_center_type_name,
        search_key,
        pv_field,
        in_oda,
        in_prodview,
        in_wellview,
        in_combo_curve
    from {{ ref('well_360') }}
),

production_aggs as (
    -- NOTE: ~18.5% of unit-months in fct_well_production_monthly have eid IS NULL
    -- (is_eid_unresolved = true). These are ProdView units without a well_360 match.
    -- Excluded intentionally — they appear in the scorecard as NULL production metrics.
    -- Monitor with: SELECT count(*) FROM fct_well_production_monthly WHERE is_eid_unresolved
    select
        eid,
        sum(oil_bbls) as cumulative_oil_bbls,
        sum(gas_mcf) as cumulative_gas_mcf,
        sum(water_bbls) as cumulative_water_bbls,
        sum(gross_boe) as cumulative_boe,
        max(gross_boe) as peak_monthly_boe,
        -- MAX_BY: non-deterministic tiebreaker if two months have equal BOE (rare in practice)
        max_by(production_month, gross_boe) as peak_month,
        min(case when gross_boe > 0 then production_month end) as first_production_month,
        max(production_month) as latest_production_month,
        count(case when gross_boe > 0 then 1 end) as producing_months_count
    from {{ ref('fct_well_production_monthly') }}
    where eid is not null
    group by eid
),

los_aggs as (
    -- int_los__well_monthly is ephemeral — compiles inline as a CTE
    -- EID derived via right(trim(well_code), 6) — trim guard prevents space-padded mismatches
    select
        eid,
        sum(los_revenue) as cumulative_los_revenue,
        sum(los_loe) as cumulative_los_loe,
        sum(los_net_income) as cumulative_los_net_income,
        count(*) as months_with_los_count
    from {{ ref('int_los__well_monthly') }}
    group by eid
),

drilling as (
    -- int_well_perf__drilling_summary is ephemeral — compiles inline
    -- Aggregates fct_daily_drilling_cost + fct_drilling_time + fct_npt_events to eid grain
    select
        eid,
        drilling_job_count,
        total_dc_cost,
        total_drilling_hours,
        total_npt_hours,
        npt_pct,
        is_npt_anomaly
    from {{ ref('int_well_perf__drilling_summary') }}
),

completion as (
    -- int_well_perf__completion_summary is ephemeral — compiles inline
    -- Aggregates fct_stimulation to eid grain
    select
        eid,
        stim_job_count,
        total_stages,
        total_proppant_lb,
        total_clean_volume_bbl,
        stim_lateral_length_ft
    from {{ ref('int_well_perf__completion_summary') }}
),

downtime_aggs as (
    -- Aggregates fct_completion_downtime (Sprint 4) to eid grain.
    -- Excludes unresolved events (eid IS NULL) — same pattern as production_aggs.
    -- downtime_event_count_12m: events with event_start_date in the rolling 12 months.
    select
        eid,
        sum(total_downtime_hours) as lifetime_downtime_hours,
        count(*) as lifetime_downtime_event_count,
        count(
            case
                when event_start_date >= dateadd('month', -12, current_date())
                    then 1
            end
        ) as downtime_event_count_12m
    from {{ ref('fct_completion_downtime') }}
    where eid is not null
    group by eid
),

daily_production_recent as (
    -- Aggregates fct_well_production_daily (Sprint 3) to eid grain.
    -- last_prod_date: most recent allocation_date with any production.
    -- 30-day averages: average daily production over the 30 most recent calendar
    --   days where gross_boe > 0 for this well.
    -- Excludes unresolved rows (eid IS NULL).
    select
        eid,
        max(allocation_date) as last_prod_date,
        -- 30-day window: average daily oil/gas over most recent 30 producing days
        avg(
            case
                when allocation_date >= dateadd('day', -30, current_date())
                    then allocated_oil_bbl
            end
        ) as avg_daily_oil_bbl_30d,
        avg(
            case
                when allocation_date >= dateadd('day', -30, current_date())
                    then allocated_gas_mcf
            end
        ) as avg_daily_gas_mcf_30d
    from {{ ref('fct_well_production_daily') }}
    where eid is not null
    group by eid
),

final as (
    select  -- noqa: ST06
        -- ── Identity (from well_360 spine) ────────────────────────────────────────
        w.eid,
        w.oda_well_id,
        w.api_10,
        w.api_14,
        w.well_name,
        w.cost_center_number,
        w.company_name,
        w.operator_name,
        w.is_operated,
        w.op_ref,
        w.basin_name,
        w.state,
        w.county,
        w.well_lateral_length_ft,
        w.wellbore_status,
        w.activity_status,
        w.spud_date,
        w.first_production_date,
        w.well_type_oda,
        w.well_configuration_type,
        w.is_revenue_generating,
        w.is_hold_billing,
        w.is_suspend_revenue,
        w.is_well,
        w.operating_group_name,
        w.cost_center_type_name,
        w.search_key,
        w.pv_field,
        w.in_oda,
        w.in_prodview,
        w.in_wellview,
        w.in_combo_curve,

        -- ── Production lifetime aggregates ────────────────────────────────────────
        p.cumulative_oil_bbls,
        p.cumulative_gas_mcf,
        p.cumulative_water_bbls,
        p.cumulative_boe,
        p.peak_monthly_boe,
        p.peak_month,
        p.first_production_month,
        p.latest_production_month,
        p.producing_months_count,

        -- ── LOS financial lifetime aggregates ─────────────────────────────────────
        l.cumulative_los_revenue,
        l.cumulative_los_loe,
        l.cumulative_los_net_income,
        l.months_with_los_count,

        -- ── Drilling lifetime aggregates ──────────────────────────────────────────
        d.drilling_job_count,
        d.total_dc_cost,
        d.total_drilling_hours,
        d.total_npt_hours,
        d.npt_pct,
        d.is_npt_anomaly,

        -- ── Completion lifetime aggregates ────────────────────────────────────────
        c.stim_job_count,
        c.total_stages,
        c.total_proppant_lb,
        c.total_clean_volume_bbl,
        c.stim_lateral_length_ft,
        -- Use well_360's fixed lateral length (not stim interval) as intensity denominator
        c.total_proppant_lb / nullif(w.well_lateral_length_ft, 0) as proppant_per_ft_lb,

        -- ── Downtime aggregates (Sprint 4) ────────────────────────────────────────
        dt.lifetime_downtime_hours,
        dt.lifetime_downtime_event_count,
        dt.downtime_event_count_12m,

        -- ── Daily production recency (Sprint 4) ───────────────────────────────────
        dp.last_prod_date,
        datediff('day', dp.last_prod_date, current_date()) as days_since_last_production,
        dp.avg_daily_oil_bbl_30d,
        dp.avg_daily_gas_mcf_30d,

        -- ── Presence flags (NULL-safe) ────────────────────────────────────────────
        -- Note: do NOT use months_with_los_count > 0 without COALESCE —
        -- that evaluates to NULL (not false) when LEFT JOIN finds no rows.
        coalesce(p.cumulative_boe, 0) > 0 as has_production_data,
        l.eid is not null as has_los_data,
        d.eid is not null as has_drilling_data,
        c.eid is not null as has_completion_data,
        dt.eid is not null as has_downtime_data,
        dp.eid is not null as has_daily_production_data

    from well_spine w
    left join production_aggs p on w.eid = p.eid
    left join los_aggs l on w.eid = l.eid
    left join drilling d on w.eid = d.eid
    left join completion c on w.eid = c.eid
    left join downtime_aggs dt on w.eid = dt.eid
    left join daily_production_recent dp on w.eid = dp.eid
)

select * from final
