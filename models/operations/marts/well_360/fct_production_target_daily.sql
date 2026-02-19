{{
    config(
        materialized='table',
        unique_key='production_target_daily_sk',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Production Target Daily
    ==============================

    PURPOSE:
    Daily production target rates per completion, with EID resolved from well_360.
    Enables target vs actual variance analysis when joined with fct_well_production_daily
    on (eid, allocation_date = target_daily_date).

    The parent target record (PVT_PVUNITCOMPTARGET) defines a target period with
    a type (Budget, Forecast, etc.) and name. The child records
    (PVT_PVUNITCOMPTARGETDAY) define per-day target rates within that period.
    This fact denormalizes both levels into a single daily grain row.

    GRAIN:
    One row per daily target record (id_rec from PVT_PVUNITCOMPTARGETDAY).
    A completion may have multiple target rows per date when multiple target
    types overlap (e.g., Budget and Actuals on the same date).

    EID RESOLUTION (four-hop via target hierarchy → completion → unit → well_360):
    1. target_daily.id_rec_parent → target.id_rec (parent target period)
    2. target.id_rec_parent → completion.id_rec (stg_prodview__completions)
    3. Primary:  completion.id_rec_parent (unit ID) → well_360.prodview_unit_id
    4. Fallback: completion.api_10 → well_360.api_10
                 (deduplicated to 1 EID per api_10 — prefer operated wells)

    DEPENDENCIES:
    - stg_prodview__production_targets_daily  (PVT_PVUNITCOMPTARGETDAY)
    - stg_prodview__production_targets         (PVT_PVUNITCOMPTARGET)
    - stg_prodview__completions                (for EID resolution)
    - well_360
#}

with

-- =============================================================================
-- DAILY TARGET RECORDS: child-level rate rows
-- =============================================================================
target_daily as (
    select
        id_rec,
        id_rec_parent,  -- FK to stg_prodview__production_targets.id_rec
        target_daily_date,
        target_daily_rate_hcliq_bbl_per_day,
        target_daily_rate_oil_bbl_per_day,
        target_daily_rate_condensate_bbl_per_day,
        target_daily_rate_ngl_bbl_per_day,
        target_daily_rate_water_bbl_per_day,
        target_daily_rate_sand_bbl_per_day,
        target_daily_rate_gas_mcf_per_day,
        created_at_utc,
        modified_at_utc,
        _fivetran_synced
    from {{ ref('stg_prodview__production_targets_daily') }}
),

-- =============================================================================
-- PARENT TARGET RECORDS: target period metadata
-- id_rec_parent here is the completion ID (FK to stg_prodview__completions.id_rec)
-- =============================================================================
target_parent as (
    select
        id_rec as target_id_rec,
        id_rec_parent as completion_id_rec,  -- completion FK
        target_start_date,
        target_type,
        cc_forecast_name,
        is_use_in_diff_from_target_calculations
    from {{ ref('stg_prodview__production_targets') }}
),

-- =============================================================================
-- COMPLETIONS: bridge for EID resolution keys
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
),

well_dim_fallback as (
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
-- JOINED: denormalize daily + parent target + completions + EID
-- =============================================================================
targets_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        td.id_rec,
        td.id_rec_parent as id_rec_target,
        tp.completion_id_rec as id_rec_comp,
        td.target_daily_date,
        tp.target_start_date,
        tp.target_type,
        tp.cc_forecast_name,
        tp.is_use_in_diff_from_target_calculations,
        td.target_daily_rate_hcliq_bbl_per_day,
        td.target_daily_rate_oil_bbl_per_day,
        td.target_daily_rate_condensate_bbl_per_day,
        td.target_daily_rate_ngl_bbl_per_day,
        td.target_daily_rate_water_bbl_per_day,
        td.target_daily_rate_sand_bbl_per_day,
        td.target_daily_rate_gas_mcf_per_day,
        td.created_at_utc,
        td.modified_at_utc,
        td._fivetran_synced

    from target_daily td
    left join target_parent tp
        on td.id_rec_parent = tp.target_id_rec
    left join completions c
        on tp.completion_id_rec = c.completion_id_rec
    left join well_dim_primary w1
        on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on c.completion_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY: surrogate key + derived BOE target rate
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }}
            as production_target_daily_sk,

        -- grain keys
        id_rec,
        id_rec_target,
        id_rec_comp,
        eid,
        is_eid_unresolved,

        -- target date + period metadata
        target_daily_date,
        target_start_date,
        target_type,
        cc_forecast_name,
        is_use_in_diff_from_target_calculations,

        -- target rates - liquids
        target_daily_rate_hcliq_bbl_per_day,
        target_daily_rate_oil_bbl_per_day,
        target_daily_rate_condensate_bbl_per_day,
        target_daily_rate_ngl_bbl_per_day,
        target_daily_rate_water_bbl_per_day,
        target_daily_rate_sand_bbl_per_day,

        -- target rates - gas
        target_daily_rate_gas_mcf_per_day,

        -- derived target BOE rate (oil + gas/6)
        coalesce(target_daily_rate_oil_bbl_per_day, 0) + (coalesce(target_daily_rate_gas_mcf_per_day, 0) / 6)
            as target_daily_rate_gross_boe_per_day,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from targets_with_eid
)

select * from final
