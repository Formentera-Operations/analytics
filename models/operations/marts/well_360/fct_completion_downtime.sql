{{
    config(
        materialized='table',
        unique_key='downtime_event_sk',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Completion Downtime
    ==============================

    PURPOSE:
    Event-level downtime fact for ProdView completions. Each row represents
    one contiguous downtime event (island of consecutive single-day records
    sharing the same downtime codes). Retains full history (no date cutoff).

    Replaces the legacy `fct_eng_completion_downtimes` functionally (legacy
    model retained until Sprint 7 deprecation). Both models coexist.

    GRAIN:
    One row per contiguous downtime event per completion. Multiple consecutive
    single-day downtime rows with the same codes are merged into one event
    via the islands+gaps algorithm (inherited from int_prodview__completion_downtimes).

    EID RESOLUTION (two-step COALESCE via completions → unit → well_360):
    1. Primary:  downtime.id_rec_parent → completions.id_rec
                 → completions.id_rec_parent (unit ID)
                 → well_360.prodview_unit_id
    2. Fallback: completions.api_10 → well_360.api_10
                 (deduplicated to 1 EID per api_10 — prefer operated wells)

    ISLANDS+GAPS:
    Single-day downtime records on consecutive dates with the same
    (id_rec_parent, downtime_code_1/2/3) are merged into one contiguous event.
    Non-single-day records (Range, Calculated) form their own events.

    DEPENDENCIES:
    - stg_prodview__completion_downtimes
    - stg_prodview__completions (for unit_id and api_10 → EID resolution)
    - well_360
#}

with

-- =============================================================================
-- RAW DOWNTIME: all records, no date cutoff (full history)
-- =============================================================================
downtime_raw as (
    select  -- noqa: ST06
        id_rec,
        id_rec_parent,
        downtime_type,
        product,
        location,
        is_failure,
        first_day,
        last_day,
        coalesce(hours_down, 0) as hours_down,
        -- Use calculated total hours when available; fall back to per-record hours
        case
            when total_downtime_hours is null then coalesce(hours_down, 0)
            else coalesce(total_downtime_hours, 0)
        end as total_downtime_hours,
        downtime_code_1,
        downtime_code_2,
        downtime_code_3,
        comments
    from {{ ref('stg_prodview__completion_downtimes') }}
),

-- =============================================================================
-- COMPLETIONS: bridge for EID resolution keys
-- id_rec_parent on the completion IS the unit's id_rec in stg_prodview__units
-- api_10 on the completion is used for the api_10 fallback path
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
    -- Primary path: ProdView unit ID → well_360.prodview_unit_id
    select
        prodview_unit_id as pvunit_id_rec,
        eid
    from {{ ref('well_360') }}
    where prodview_unit_id is not null
),

well_dim_fallback as (
    -- Fallback path: API-10 → well_360.api_10
    -- Deduplicated to 1 EID per api_10: prefer operated wells, then lowest EID
    select
        api_10 as pvunit_api_10,
        eid
    from {{ ref('well_360') }}
    where api_10 is not null
    qualify row_number() over (
        partition by api_10
        order by
            case when is_operated then 0 else 1 end,
            eid
    ) = 1
),

-- =============================================================================
-- DOWNTIME WITH EID: join completions for unit_id, resolve EID
-- =============================================================================
downtime_with_eid as (
    select  -- noqa: ST06
        -- EID resolution
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        -- downtime record fields
        d.id_rec,
        d.id_rec_parent,
        d.downtime_type,
        d.product,
        d.location,
        d.is_failure,
        d.first_day,
        d.last_day,
        d.hours_down,
        d.total_downtime_hours,
        d.downtime_code_1,
        d.downtime_code_2,
        d.downtime_code_3,
        d.comments

    from downtime_raw d
    left join completions c
        on d.id_rec_parent = c.completion_id_rec
    left join well_dim_primary w1
        on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on c.completion_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- ISLANDS+GAPS: detect consecutive single-day downtime events
-- Partition: (id_rec_parent, downtime_code_1/2/3) — same completion, same codes
-- =============================================================================
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
    from downtime_with_eid
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

-- =============================================================================
-- ISLAND METADATA: compute event start/end/total across each island
-- =============================================================================
island_metadata as (
    select
        *,
        -- Event identifier: min id_rec within the island (stable, natural key)
        min(id_rec) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as downtime_event_id,
        min(first_day) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as event_start_date,
        max(last_day) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as event_end_date,
        sum(total_downtime_hours) over (
            partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        ) as event_total_downtime_hours
    from islands
),

-- =============================================================================
-- COLLAPSE TO ISLAND GRAIN: one row per contiguous downtime event
-- All window-computed columns (event_start_date, event_end_date,
-- event_total_downtime_hours, downtime_event_id) are identical within
-- the same island partition, so QUALIFY on the min id_rec is sufficient.
-- =============================================================================
events as (
    select  -- noqa: ST06
        downtime_event_id,
        id_rec_parent as id_rec_comp,
        eid,
        is_eid_unresolved,
        event_start_date,
        event_end_date,
        event_total_downtime_hours,
        datediff('day', event_start_date, coalesce(event_end_date, current_date() - 1)) as days_down,
        downtime_code_1,
        downtime_code_2,
        downtime_code_3,
        downtime_type,
        product,
        location,
        is_failure,
        comments
    from island_metadata
    qualify row_number() over (
        partition by id_rec_parent, downtime_code_1, downtime_code_2, downtime_code_3, island_id
        order by id_rec
    ) = 1
),

-- =============================================================================
-- FINAL ASSEMBLY: add surrogate key + metadata
-- =============================================================================
final as (
    select
        -- surrogate key: event-level, based on stable event identifier
        {{ dbt_utils.generate_surrogate_key(['downtime_event_id']) }}
            as downtime_event_sk,

        -- grain key
        downtime_event_id,

        -- FK bridges
        id_rec_comp,

        -- EID resolution
        eid,
        is_eid_unresolved,

        -- event timing
        event_start_date,
        event_end_date,
        event_total_downtime_hours as total_downtime_hours,
        days_down,

        -- downtime classification
        downtime_code_1,
        downtime_code_2,
        downtime_code_3,
        downtime_type,
        product,
        location,
        is_failure,
        comments,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from events
)

select * from final
