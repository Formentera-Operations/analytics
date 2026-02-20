{{
    config(
        materialized='table',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Artificial Lift Daily
    ==============================

    PURPOSE:
    Daily operational readings for artificial lift installations per ProdView
    completion. A single model covering two lift types via UNION with a
    `lift_type` discriminator ('plunger' | 'rod_pump'). Enables artificial
    lift performance analytics: trip counts, travel time, SPM trends, and
    efficiency monitoring per well.

    GRAIN:
    One row per daily reading per lift installation. Lift_type + id_rec is
    the natural key (id_rec alone is unique within each source table but not
    across both; the surrogate key uses both).

    LIFT TYPE HIERARCHY (both types go through artificial_lift header):

    Plunger (confirmed by data — completion_id maps to artificial_lift, not completions):
      stg_prodview__plunger_lift_readings.plunger_lift_id
        → stg_prodview__plunger_lifts.id_rec
        → stg_prodview__plunger_lifts.completion_id (= pvUnitCompPump.idrec)
        → stg_prodview__artificial_lift.id_rec
        → stg_prodview__artificial_lift.id_rec_parent (pvUnitComp completion_id)
        → stg_prodview__completions.id_rec_parent (unit_id)
        → well_360.prodview_unit_id

    Rod pump (4-hop to completion):
      stg_prodview__rod_pump_entries.id_rec_parent
        → stg_prodview__rod_pump_configs.id_rec
        → stg_prodview__rod_pump_configs.id_rec_parent
        → stg_prodview__artificial_lift.id_rec (pump header/pvUnitCompPump)
        → stg_prodview__artificial_lift.id_rec_parent (pvUnitComp completion_id)
        → stg_prodview__completions.id_rec_parent (unit_id)
        → well_360.prodview_unit_id

    ROWS: ~79K total (61K plunger + 18K rod pump) as of Sprint 9.
    Materialization: table (well under incremental threshold).

    DEPENDENCIES:
    - stg_prodview__plunger_lift_readings
    - stg_prodview__plunger_lifts
    - stg_prodview__rod_pump_entries
    - stg_prodview__rod_pump_configs
    - stg_prodview__artificial_lift
    - stg_prodview__completions (EID bridge)
    - well_360
#}

with

-- =============================================================================
-- PLUNGER: readings joined to lift header to get completion_id
-- =============================================================================
plunger_raw as (
    select
        r.id_rec,
        'plunger' as lift_type,
        al.id_rec_parent as id_rec_comp,

        -- dates
        r.entry_date as reading_date,

        -- operating parameters
        r.plunger_on_pressure_psi as on_pressure_psi,
        null::float as off_pressure_psi,

        -- trip / cycle counts (plunger-specific)
        r.total_trips as total_trips_or_cycles,
        r.successful_trips as successful_trips_or_arrivals,
        r.failed_trips as failed_trips_or_misses,

        -- duration metrics (plunger-specific)
        r.duration_on_minutes,
        r.duration_off_minutes,
        r.after_flow_time_hours,

        -- travel time (plunger-specific; NULL for rod pump)
        r.travel_time_avg_minutes,
        r.travel_time_max_minutes,
        r.travel_time_min_minutes,
        r.travel_time_target_minutes,

        -- rod pump specific fields (NULL for plunger)
        null::float as spm,
        null::float as stroke_length_in,
        null::float as run_time_pct,
        null::float as vol_per_day_calc_bbl,

        -- audit
        r.created_by,
        r.created_at_utc,
        r.modified_by,
        r.modified_at_utc,
        r.comments

    from {{ ref('stg_prodview__plunger_lift_readings') }} r
    inner join {{ ref('stg_prodview__plunger_lifts') }} pl
        on r.plunger_lift_id = pl.id_rec
    inner join {{ ref('stg_prodview__artificial_lift') }} al
        on pl.completion_id = al.id_rec
    where lower(trim(r.comments)) != 'seed record' or r.comments is null
),

-- =============================================================================
-- ROD PUMP: entries → configs → artificial_lift_header to get completion_id
-- =============================================================================
rod_pump_raw as (
    select
        e.id_rec,
        'rod_pump' as lift_type,
        al.id_rec_parent as id_rec_comp,

        -- dates
        e.observation_date as reading_date,

        -- operating parameters
        null::float as on_pressure_psi,
        null::float as off_pressure_psi,

        -- trip / cycle counts (NULL for rod pump)
        null::float as total_trips_or_cycles,
        null::float as successful_trips_or_arrivals,
        null::float as failed_trips_or_misses,

        -- duration metrics (NULL for rod pump)
        null::float as duration_on_minutes,
        null::float as duration_off_minutes,
        null::float as after_flow_time_hours,

        -- travel time (NULL for rod pump)
        null::float as travel_time_avg_minutes,
        null::float as travel_time_max_minutes,
        null::float as travel_time_min_minutes,
        null::float as travel_time_target_minutes,

        -- rod pump specific fields
        e.spm,
        e.stroke_length_in,
        e.run_time_pct,
        e.vol_per_day_calc_bbl,

        -- audit
        e.created_by,
        e.created_at_utc,
        e.modified_by,
        e.modified_at_utc,
        e.comments

    from {{ ref('stg_prodview__rod_pump_entries') }} e
    inner join {{ ref('stg_prodview__rod_pump_configs') }} rc
        on e.id_rec_parent = rc.id_rec
    inner join {{ ref('stg_prodview__artificial_lift') }} al
        on rc.id_rec_parent = al.id_rec
    where not e.is_seed_record
),

-- =============================================================================
-- UNION all lift types
-- =============================================================================
lift_readings as (
    select * from plunger_raw
    union all
    select * from rod_pump_raw
),

-- =============================================================================
-- COMPLETIONS: bridge for EID resolution
-- =============================================================================
completions as (
    select
        id_rec as completion_id_rec,
        id_rec_parent as unit_id_rec,
        api_10 as completion_api_10
    from {{ ref('stg_prodview__completions') }}
),

-- =============================================================================
-- EID RESOLUTION: deduplicated lookups from well_360
-- =============================================================================
well_dim_primary as (
    select
        prodview_unit_id as pvunit_id_rec,
        eid
    from {{ ref('well_360') }}
    where prodview_unit_id is not null
    qualify row_number() over (
        partition by prodview_unit_id
        order by case when is_operated then 0 else 1 end, eid
    ) = 1
),

well_dim_fallback as (
    select  -- noqa: ST06
        nullif(api_10, '') as pvunit_api_10,
        eid
    from {{ ref('well_360') }}
    where nullif(api_10, '') is not null
    qualify row_number() over (
        partition by nullif(api_10, '')
        order by case when is_operated then 0 else 1 end, eid
    ) = 1
),

-- =============================================================================
-- READINGS WITH EID
-- =============================================================================
readings_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        lr.id_rec,
        lr.lift_type,
        lr.id_rec_comp,
        lr.reading_date,

        -- pressure
        lr.on_pressure_psi,
        lr.off_pressure_psi,

        -- trip / cycle counts
        lr.total_trips_or_cycles,
        lr.successful_trips_or_arrivals,
        lr.failed_trips_or_misses,

        -- duration metrics (plunger)
        lr.duration_on_minutes,
        lr.duration_off_minutes,
        lr.after_flow_time_hours,

        -- travel time (plunger)
        lr.travel_time_avg_minutes,
        lr.travel_time_max_minutes,
        lr.travel_time_min_minutes,
        lr.travel_time_target_minutes,

        -- rod pump specifics
        lr.spm,
        lr.stroke_length_in,
        lr.run_time_pct,
        lr.vol_per_day_calc_bbl,

        -- audit
        lr.created_by,
        lr.created_at_utc,
        lr.modified_by,
        lr.modified_at_utc,
        lr.comments

    from lift_readings lr
    left join completions c
        on lr.id_rec_comp = c.completion_id_rec
    left join well_dim_primary w1
        on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on c.completion_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec', 'lift_type']) }}
            as artificial_lift_daily_sk,

        -- grain key
        id_rec,
        lift_type,

        -- FK bridge
        id_rec_comp,

        -- EID resolution
        eid,
        is_eid_unresolved,

        -- event date
        reading_date,

        -- pressure measurements (both lift types where applicable)
        on_pressure_psi,
        off_pressure_psi,

        -- plunger: trip / cycle counts
        total_trips_or_cycles,
        successful_trips_or_arrivals,
        failed_trips_or_misses,

        -- plunger: duration metrics
        duration_on_minutes,
        duration_off_minutes,
        after_flow_time_hours,

        -- plunger: travel time metrics
        travel_time_avg_minutes,
        travel_time_max_minutes,
        travel_time_min_minutes,
        travel_time_target_minutes,

        -- rod pump: operational metrics
        spm,
        stroke_length_in,
        run_time_pct,
        vol_per_day_calc_bbl,

        -- audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        comments,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from readings_with_eid
)

select * from final
