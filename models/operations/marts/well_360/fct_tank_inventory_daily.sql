{{
    config(
        materialized='table',
        unique_key='tank_inventory_daily_sk',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Tank Inventory Daily
    ==============================

    PURPOSE:
    Daily tank inventory volumes per ProdView tank, with EID resolved from well_360.
    Provides opening/closing/change-in-inventory for oil+condensate, water, and
    sand at the tank level, linked to the well that owns the tank.

    Replaces fct_eng_tank_inventories functionally (legacy model retained until
    Sprint 7 deprecation). Key improvements:
    - Full history (legacy has a 3-year rolling window filter)
    - EID resolved (legacy only carries unit_id and unit_type, no well_360 join)
    - No facility_id IS NOT NULL filter (retains all tanks)
    - Tank attributes from stg_prodview__tanks denormalized in for self-contained analysis

    GRAIN:
    One row per daily tank volume record (id_rec from PVT_PVUNITTANKMONTHDAYCALC).

    EID RESOLUTION (via tanks → units → well_360):
    1. tank_daily_volumes.tank_id → tanks.id_rec
       → tanks.id_rec_parent (unit ID)
    2. Primary:  tanks.id_rec_parent → well_360.prodview_unit_id
    3. Fallback: units.api_10 → well_360.api_10
       (requires joining stg_prodview__units for api_10 since tanks don't carry it)

    NOTE: Tanks are associated with UNITS (pvunit/pvunitcomp) in ProdView.
    Tanks linked to facility units (pvfac) will not resolve an EID and will have
    is_eid_unresolved = true. These represent battery or facility-level tanks
    that will be covered by fct_facility_monthly in Sprint 7.

    DEPENDENCIES:
    - stg_prodview__tank_daily_volumes
    - stg_prodview__tanks               (for unit_id + tank attributes)
    - stg_prodview__units               (for api_10 fallback)
    - well_360
#}

with

-- =============================================================================
-- DAILY TANK VOLUMES: inventory measurements
-- =============================================================================
tank_volumes as (
    select
        id_rec,
        tank_id,
        tank_date::date as tank_date,
        tank_year,
        tank_month,
        day_of_month,
        opening_total_volume_bbl,
        opening_oil_condensate_volume_bbl,
        opening_water_volume_bbl,
        opening_sand_volume_bbl,
        opening_bsw_total_pct,
        opening_sand_cut_total_pct,
        closing_total_volume_bbl,
        closing_oil_condensate_volume_bbl,
        closing_water_volume_bbl,
        closing_sand_volume_bbl,
        closing_bsw_total_pct,
        closing_sand_cut_total_pct,
        change_total_volume_bbl,
        change_oil_condensate_volume_bbl,
        change_water_volume_bbl,
        change_sand_volume_bbl,
        current_facility_id,
        created_at_utc,
        modified_at_utc,
        _fivetran_synced
    from {{ ref('stg_prodview__tank_daily_volumes') }}
),

-- =============================================================================
-- TANKS: dimension attributes + unit_id for EID resolution
-- id_rec_parent on the tank IS the unit's id_rec (pvunit/pvunitcomp/pvfac)
-- =============================================================================
tanks as (
    select
        id_rec as tank_id_rec,
        id_rec_parent as unit_id_rec,  -- unit FK for EID resolution
        tank_name,
        product as tank_product,
        tank_capacity_bbl,
        exclude_from_production,
        is_active,
        start_using_tank,
        stop_using_tank
    from {{ ref('stg_prodview__tanks') }}
),

-- =============================================================================
-- UNITS: needed for api_10 to support fallback EID resolution
-- Tanks don't carry api_10 directly; must join through units
-- =============================================================================
units as (
    select
        id_rec as unit_id_rec,
        api_10
    from {{ ref('stg_prodview__units') }}
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
-- JOINED: volumes + tank attrs + EID resolution
-- =============================================================================
volumes_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        v.id_rec,
        v.tank_id,
        t.tank_name,
        t.tank_product,
        t.tank_capacity_bbl,
        t.exclude_from_production,
        t.is_active as is_tank_active,
        t.unit_id_rec as id_rec_unit,
        v.tank_date,
        v.tank_year,
        v.tank_month,
        v.day_of_month,
        v.opening_total_volume_bbl,
        v.opening_oil_condensate_volume_bbl,
        v.opening_water_volume_bbl,
        v.opening_sand_volume_bbl,
        v.opening_bsw_total_pct,
        v.opening_sand_cut_total_pct,
        v.closing_total_volume_bbl,
        v.closing_oil_condensate_volume_bbl,
        v.closing_water_volume_bbl,
        v.closing_sand_volume_bbl,
        v.closing_bsw_total_pct,
        v.closing_sand_cut_total_pct,
        v.change_total_volume_bbl,
        v.change_oil_condensate_volume_bbl,
        v.change_water_volume_bbl,
        v.change_sand_volume_bbl,
        v.current_facility_id,
        v.created_at_utc,
        v.modified_at_utc,
        v._fivetran_synced

    from tank_volumes v
    left join tanks t
        on v.tank_id = t.tank_id_rec
    left join units u
        on t.unit_id_rec = u.unit_id_rec
    left join well_dim_primary w1
        on t.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on u.api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY: surrogate key
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }}
            as tank_inventory_daily_sk,

        -- grain keys
        id_rec,
        tank_id,
        id_rec_unit,
        eid,
        is_eid_unresolved,
        tank_date,

        -- tank attributes (denormalized for self-contained analysis)
        tank_name,
        tank_product,
        tank_capacity_bbl,
        exclude_from_production,
        is_tank_active,

        -- date components
        tank_year,
        tank_month,
        day_of_month,

        -- opening inventory
        opening_total_volume_bbl,
        opening_oil_condensate_volume_bbl,
        opening_water_volume_bbl,
        opening_sand_volume_bbl,
        opening_bsw_total_pct,
        opening_sand_cut_total_pct,

        -- closing inventory
        closing_total_volume_bbl,
        closing_oil_condensate_volume_bbl,
        closing_water_volume_bbl,
        closing_sand_volume_bbl,
        closing_bsw_total_pct,
        closing_sand_cut_total_pct,

        -- change in inventory
        change_total_volume_bbl,
        change_oil_condensate_volume_bbl,
        change_water_volume_bbl,
        change_sand_volume_bbl,

        -- facility reference
        current_facility_id,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from volumes_with_eid
)

select * from final
