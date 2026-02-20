{{
    config(
        materialized='table',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Completion Status History
    ==============================

    PURPOSE:
    Effective-dated status change events per ProdView completion. Each row
    represents one pvUnitCompStatus record — a point-in-time snapshot of a
    completion's operating status (producing, shut-in, injecting, etc.).
    Retains full history (no date cutoff).

    Complements fct_completion_downtime for root cause analysis and well
    lifecycle surveillance workflows. Status records describe WHAT a well is
    doing; downtime records describe WHEN it stopped doing it.

    GRAIN:
    One row per status record (PVT_PVUNITCOMPSTATUS.IDREC). Status changes
    are infrequent — typically < 20 per well over its lifetime.

    EID RESOLUTION (two-step COALESCE via completions → unit → well_360):
    1. Primary:  status.id_rec_parent → completions.id_rec
                 → completions.id_rec_parent (unit ID)
                 → well_360.prodview_unit_id
    2. Fallback: completions.api_10 → well_360.api_10
                 (deduplicated to 1 EID per api_10 — prefer operated wells)

    DEPENDENCIES:
    - stg_prodview__status
    - stg_prodview__completions (for unit_id and api_10 → EID resolution)
    - well_360
#}

with

-- =============================================================================
-- RAW STATUS: all records, no date cutoff (full history)
-- =============================================================================
status_raw as (
    select  -- noqa: ST06
        id_rec,
        id_rec_parent,
        id_flownet,
        status_date,
        status,
        primary_fluid_type,
        flow_direction,
        commingled,
        oil_or_condensate,
        completion_type,
        production_method,
        calc_lost_production,
        include_in_well_count,
        comments,
        user_txt1,
        user_txt2,
        user_txt3,
        user_num1,
        user_num2,
        user_num3,
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc
    from {{ ref('stg_prodview__status') }}
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
    -- Deduplicated to 1 EID per prodview_unit_id: prefer operated wells, then lowest EID
    -- (guards against duplicate prodview_unit_id across two EIDs in well_360)
    select
        prodview_unit_id as pvunit_id_rec,
        eid
    from {{ ref('well_360') }}
    where prodview_unit_id is not null
    qualify row_number() over (
        partition by prodview_unit_id
        order by
            case when is_operated then 0 else 1 end,
            eid
    ) = 1
),

well_dim_fallback as (
    -- Fallback path: API-10 → well_360.api_10
    -- Deduplicated to 1 EID per api_10: prefer operated wells, then lowest EID
    -- nullif guards against blank-string api_10 being grouped into a single partition
    select  -- noqa: ST06
        nullif(api_10, '') as pvunit_api_10,
        eid
    from {{ ref('well_360') }}
    where nullif(api_10, '') is not null
    qualify row_number() over (
        partition by nullif(api_10, '')
        order by
            case when is_operated then 0 else 1 end,
            eid
    ) = 1
),

-- =============================================================================
-- STATUS WITH EID: join completions for unit_id, resolve EID
-- =============================================================================
status_with_eid as (
    select  -- noqa: ST06
        -- EID resolution
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        -- status record fields
        s.id_rec,
        s.id_rec_parent as id_rec_comp,
        s.id_flownet,
        s.status_date,
        s.status,
        s.primary_fluid_type,
        s.flow_direction,
        s.commingled,
        s.oil_or_condensate,
        s.completion_type,
        s.production_method,
        s.calc_lost_production,
        s.include_in_well_count,
        s.comments,
        s.user_txt1,
        s.user_txt2,
        s.user_txt3,
        s.user_num1,
        s.user_num2,
        s.user_num3,
        s.created_by,
        s.created_at_utc,
        s.modified_by,
        s.modified_at_utc

    from status_raw s
    left join completions c
        on s.id_rec_parent = c.completion_id_rec
    left join well_dim_primary w1
        on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on c.completion_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY: add surrogate key + metadata
-- =============================================================================
final as (
    select
        -- surrogate key: record-level, based on natural key
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }}
            as completion_status_sk,

        -- grain key
        id_rec,

        -- FK bridges
        id_rec_comp,
        id_flownet,

        -- EID resolution
        eid,
        is_eid_unresolved,

        -- status event
        status_date,
        status,

        -- completion characteristics at this status snapshot
        primary_fluid_type,
        flow_direction,
        commingled,
        oil_or_condensate,
        completion_type,
        production_method,

        -- reporting flags
        calc_lost_production,
        include_in_well_count,

        -- comments and user-defined fields
        comments,
        user_txt1,
        user_txt2,
        user_txt3,
        user_num1,
        user_num2,
        user_num3,

        -- audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from status_with_eid
)

select * from final
