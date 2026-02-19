{{
    config(
        materialized='incremental',
        unique_key='well_production_daily_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['eid', 'allocation_date'],
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Well Production Daily
    ==============================

    PURPOSE:
    Daily production volumes per ProdView unit, with EID resolved from well_360.
    Wide column set — all mart-relevant staging columns carried through at the
    source grain (unit × day), with no volume aggregation. Downstream analysts
    can GROUP BY eid when they need well-level daily summaries.

    This is the EID-resolved replacement for fct_eng_volumes (legacy, retained
    until Sprint 7 deprecation). Both models coexist as independent facts.

    GRAIN:
    One row per (id_rec_unit, allocation_date). This is the source grain from
    ProdView's daily allocation table.

    For resolved wells, eid is populated and id_rec_unit is also carried (for
    ProdView-side debugging and FK bridge context). For unresolved wells, eid
    is NULL and id_rec_unit is the only grain discriminator.

    EID RESOLUTION (two-step COALESCE, ~81.5% match on pvunitcomp units):
    1. Primary:  stg_prodview__units.id_rec = well_360.prodview_unit_id
    2. Fallback: stg_prodview__units.api_10 = well_360.api_10
       (deduplicated to 1 EID per api_10 — prefer operated wells to avoid fan-out)

    INCREMENTAL STRATEGY:
    Watermark: _fivetran_synced (stable Fivetran ingestion timestamp).
    _loaded_at from the staging VIEW always returns current_timestamp() and is
    NOT usable as an incremental watermark.
    Initial build: dbt build --select fct_well_production_daily --full-refresh

    UPSTREAM FILTER:
    stg_prodview__units is filtered to unit_type = 'pvunitcomp' only.
    This excludes facilities, external ins/outs, and meters (~3K units).

    DEPENDENCIES:
    - stg_prodview__daily_allocations
    - stg_prodview__units
    - well_360
#}

with

-- =============================================================================
-- DAILY ALLOCATIONS: full mart-relevant column set + incremental filter
-- =============================================================================
daily_allocations as (
    select
        -- identifiers
        id_rec,
        id_rec_unit,
        id_rec_comp,
        id_rec_comp_zone,
        id_flownet,

        -- date/time
        allocation_date,
        allocation_year,
        allocation_month,
        allocation_day_of_month,

        -- operational time
        downtime_hours,
        operating_time_hours,

        -- gathered volumes
        gathered_hcliq_bbl,
        gathered_gas_mcf,
        gathered_water_bbl,
        gathered_sand_bbl,

        -- allocated volumes
        allocated_hcliq_bbl,
        allocated_oil_bbl,
        allocated_condensate_bbl,
        allocated_ngl_bbl,
        allocated_gas_eq_hcliq_mcf,
        allocated_gas_mcf,
        allocated_water_bbl,
        allocated_sand_bbl,

        -- allocation factors
        alloc_factor_hcliq,
        alloc_factor_gas,
        alloc_factor_water,
        alloc_factor_sand,

        -- new production volumes
        new_prod_hcliq_bbl,
        new_prod_oil_bbl,
        new_prod_condensate_bbl,
        new_prod_ngl_bbl,
        new_prod_hcliq_gas_eq_mcf,
        new_prod_gas_mcf,
        new_prod_water_bbl,
        new_prod_sand_bbl,

        -- working interest
        wi_hcliq_pct,
        wi_gas_pct,
        wi_water_pct,
        wi_sand_pct,

        -- net revenue interest
        nri_hcliq_pct,
        nri_gas_pct,
        nri_water_pct,
        nri_sand_pct,

        -- deferred production
        deferred_hcliq_bbl,
        deferred_gas_mcf,
        deferred_water_bbl,
        deferred_sand_bbl,

        -- difference from target
        diff_target_hcliq_bbl,
        diff_target_oil_bbl,
        diff_target_condensate_bbl,
        diff_target_ngl_bbl,
        diff_target_gas_mcf,
        diff_target_water_bbl,
        diff_target_sand_bbl,

        -- dispositions - sales
        disp_sales_hcliq_bbl,
        disp_sales_oil_bbl,
        disp_sales_condensate_bbl,
        disp_sales_ngl_bbl,
        disp_sales_gas_mcf,

        -- dispositions - gas uses
        disp_fuel_gas_mcf,
        disp_flare_gas_mcf,
        disp_incineration_gas_mcf,
        disp_vent_gas_mcf,
        disp_injected_gas_mcf,
        disp_injected_water_bbl,

        -- injection well volumes
        injection_well_hcliq_bbl,
        injection_well_gas_mcf,
        injection_well_water_bbl,
        injection_well_sand_bbl,

        -- cumulative production
        cum_hcliq_bbl,
        cum_oil_bbl,
        cum_condensate_bbl,
        cum_ngl_bbl,
        cum_gas_mcf,
        cum_water_bbl,
        cum_sand_bbl,

        -- heat content
        gathered_heat_mmbtu,
        gathered_heat_factor_btu_per_ft3,
        allocated_heat_mmbtu,
        allocated_heat_factor_btu_per_ft3,
        new_prod_heat_mmbtu,
        disp_sales_heat_mmbtu,
        disp_fuel_heat_mmbtu,
        disp_flare_heat_mmbtu,
        disp_vent_heat_mmbtu,
        disp_incinerate_heat_mmbtu,

        -- density
        allocated_density_api,
        sales_density_api,

        -- FK bridges (for future event-level joins)
        id_rec_meas_method,
        id_rec_test,
        id_rec_param,
        id_rec_downtime,
        id_rec_deferment,
        id_rec_status,
        id_rec_facility,

        -- operational metrics
        pump_efficiency_pct,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced

    from {{ ref('stg_prodview__daily_allocations') }}
    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}
    -- Deduplicate to one record per (id_rec_unit, allocation_date).
    -- The staging model deduplicates by idrec only; ProdView can emit multiple
    -- idrec values for the same unit-date (e.g., Fivetran re-ingestion of
    -- re-allocated recent records). Keep the latest-synced authoritative record.
    qualify row_number() over (
        partition by id_rec_unit, allocation_date
        order by _fivetran_synced desc, id_rec asc
    ) = 1
),

-- =============================================================================
-- UNITS: filter to wells-only, carry EID resolution keys
-- =============================================================================
units as (
    select
        id_rec,
        api_10
    from {{ ref('stg_prodview__units') }}
    where unit_type = 'pvunitcomp'
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
    -- (multiple completions can share an api_10; fan-out would double volumes)
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
-- PRODUCTION WITH EID: join units + resolve EID (no aggregation — unit grain)
-- EID is populated for resolved wells; NULL for unresolved.
-- id_rec_unit is carried for ALL rows for FK bridge context and debugging.
-- =============================================================================
production_with_eid as (
    select  -- noqa: ST06
        -- EID resolution
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        -- pass all staging columns through at unit grain
        a.id_rec,
        a.id_rec_unit,
        a.id_rec_comp,
        a.id_rec_comp_zone,
        a.id_flownet,
        a.allocation_date,
        a.allocation_year,
        a.allocation_month,
        a.allocation_day_of_month,
        a.downtime_hours,
        a.operating_time_hours,
        a.gathered_hcliq_bbl,
        a.gathered_gas_mcf,
        a.gathered_water_bbl,
        a.gathered_sand_bbl,
        a.allocated_hcliq_bbl,
        a.allocated_oil_bbl,
        a.allocated_condensate_bbl,
        a.allocated_ngl_bbl,
        a.allocated_gas_eq_hcliq_mcf,
        a.allocated_gas_mcf,
        a.allocated_water_bbl,
        a.allocated_sand_bbl,
        a.alloc_factor_hcliq,
        a.alloc_factor_gas,
        a.alloc_factor_water,
        a.alloc_factor_sand,
        a.new_prod_hcliq_bbl,
        a.new_prod_oil_bbl,
        a.new_prod_condensate_bbl,
        a.new_prod_ngl_bbl,
        a.new_prod_hcliq_gas_eq_mcf,
        a.new_prod_gas_mcf,
        a.new_prod_water_bbl,
        a.new_prod_sand_bbl,
        a.wi_hcliq_pct,
        a.wi_gas_pct,
        a.wi_water_pct,
        a.wi_sand_pct,
        a.nri_hcliq_pct,
        a.nri_gas_pct,
        a.nri_water_pct,
        a.nri_sand_pct,
        a.deferred_hcliq_bbl,
        a.deferred_gas_mcf,
        a.deferred_water_bbl,
        a.deferred_sand_bbl,
        a.diff_target_hcliq_bbl,
        a.diff_target_oil_bbl,
        a.diff_target_condensate_bbl,
        a.diff_target_ngl_bbl,
        a.diff_target_gas_mcf,
        a.diff_target_water_bbl,
        a.diff_target_sand_bbl,
        a.disp_sales_hcliq_bbl,
        a.disp_sales_oil_bbl,
        a.disp_sales_condensate_bbl,
        a.disp_sales_ngl_bbl,
        a.disp_sales_gas_mcf,
        a.disp_fuel_gas_mcf,
        a.disp_flare_gas_mcf,
        a.disp_incineration_gas_mcf,
        a.disp_vent_gas_mcf,
        a.disp_injected_gas_mcf,
        a.disp_injected_water_bbl,
        a.injection_well_hcliq_bbl,
        a.injection_well_gas_mcf,
        a.injection_well_water_bbl,
        a.injection_well_sand_bbl,
        a.cum_hcliq_bbl,
        a.cum_oil_bbl,
        a.cum_condensate_bbl,
        a.cum_ngl_bbl,
        a.cum_gas_mcf,
        a.cum_water_bbl,
        a.cum_sand_bbl,
        a.gathered_heat_mmbtu,
        a.gathered_heat_factor_btu_per_ft3,
        a.allocated_heat_mmbtu,
        a.allocated_heat_factor_btu_per_ft3,
        a.new_prod_heat_mmbtu,
        a.disp_sales_heat_mmbtu,
        a.disp_fuel_heat_mmbtu,
        a.disp_flare_heat_mmbtu,
        a.disp_vent_heat_mmbtu,
        a.disp_incinerate_heat_mmbtu,
        a.allocated_density_api,
        a.sales_density_api,
        a.id_rec_meas_method,
        a.id_rec_test,
        a.id_rec_param,
        a.id_rec_downtime,
        a.id_rec_deferment,
        a.id_rec_status,
        a.id_rec_facility,
        a.pump_efficiency_pct,
        a.created_at_utc,
        a.modified_at_utc,
        a._fivetran_synced

    from daily_allocations a
    inner join units u
        on a.id_rec_unit = u.id_rec
    left join well_dim_primary w1
        on u.id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on u.api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY: surrogate key + BOE calculation
-- =============================================================================
final as (
    select
        -- surrogate key: unit × date — uniqueness at the source grain.
        -- grain is (id_rec_unit, allocation_date) for ALL rows (resolved and
        -- unresolved). Using id_rec_unit directly avoids the EID-coalesce
        -- collision where multiple units sharing the same EID would produce
        -- identical keys on the same date.
        {{ dbt_utils.generate_surrogate_key(['id_rec_unit', 'allocation_date']) }}
            as well_production_daily_sk,

        -- grain keys
        eid,
        id_rec_unit,
        allocation_date,

        -- EID resolution flag
        is_eid_unresolved,

        -- source record ID
        id_rec,

        -- completion / zone references
        id_rec_comp,
        id_rec_comp_zone,
        id_flownet,

        -- date components (convenience columns for BI partitioning)
        allocation_year,
        allocation_month,
        allocation_day_of_month,

        -- operational time
        downtime_hours,
        operating_time_hours,

        -- gathered volumes
        gathered_hcliq_bbl,
        gathered_gas_mcf,
        gathered_water_bbl,
        gathered_sand_bbl,

        -- allocated volumes
        allocated_hcliq_bbl,
        allocated_oil_bbl,
        allocated_condensate_bbl,
        allocated_ngl_bbl,
        allocated_gas_eq_hcliq_mcf,
        allocated_gas_mcf,
        allocated_water_bbl,
        allocated_sand_bbl,

        -- BOE (daily): oil + gas/6 (industry-standard 6:1 conversion)
        coalesce(allocated_oil_bbl, 0) + (coalesce(allocated_gas_mcf, 0) / 6)
            as gross_boe,

        -- allocation factors
        alloc_factor_hcliq,
        alloc_factor_gas,
        alloc_factor_water,
        alloc_factor_sand,

        -- new production volumes
        new_prod_hcliq_bbl,
        new_prod_oil_bbl,
        new_prod_condensate_bbl,
        new_prod_ngl_bbl,
        new_prod_hcliq_gas_eq_mcf,
        new_prod_gas_mcf,
        new_prod_water_bbl,
        new_prod_sand_bbl,

        -- working interest
        wi_hcliq_pct,
        wi_gas_pct,
        wi_water_pct,
        wi_sand_pct,

        -- net revenue interest
        nri_hcliq_pct,
        nri_gas_pct,
        nri_water_pct,
        nri_sand_pct,

        -- deferred production
        deferred_hcliq_bbl,
        deferred_gas_mcf,
        deferred_water_bbl,
        deferred_sand_bbl,

        -- variance from target
        diff_target_hcliq_bbl,
        diff_target_oil_bbl,
        diff_target_condensate_bbl,
        diff_target_ngl_bbl,
        diff_target_gas_mcf,
        diff_target_water_bbl,
        diff_target_sand_bbl,

        -- dispositions - sales
        disp_sales_hcliq_bbl,
        disp_sales_oil_bbl,
        disp_sales_condensate_bbl,
        disp_sales_ngl_bbl,
        disp_sales_gas_mcf,

        -- dispositions - gas uses
        disp_fuel_gas_mcf,
        disp_flare_gas_mcf,
        disp_incineration_gas_mcf,
        disp_vent_gas_mcf,
        disp_injected_gas_mcf,
        disp_injected_water_bbl,

        -- injection well volumes
        injection_well_hcliq_bbl,
        injection_well_gas_mcf,
        injection_well_water_bbl,
        injection_well_sand_bbl,

        -- cumulative production (ProdView running totals at unit level)
        cum_hcliq_bbl,
        cum_oil_bbl,
        cum_condensate_bbl,
        cum_ngl_bbl,
        cum_gas_mcf,
        cum_water_bbl,
        cum_sand_bbl,

        -- heat content
        gathered_heat_mmbtu,
        gathered_heat_factor_btu_per_ft3,
        allocated_heat_mmbtu,
        allocated_heat_factor_btu_per_ft3,
        new_prod_heat_mmbtu,
        disp_sales_heat_mmbtu,
        disp_fuel_heat_mmbtu,
        disp_flare_heat_mmbtu,
        disp_vent_heat_mmbtu,
        disp_incinerate_heat_mmbtu,

        -- density
        allocated_density_api,
        sales_density_api,

        -- FK bridges (for event-level joins in Sprint 4+)
        id_rec_meas_method,
        id_rec_test,
        id_rec_param,
        id_rec_downtime,
        id_rec_deferment,
        id_rec_status,
        id_rec_facility,

        -- operational metrics
        pump_efficiency_pct,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from production_with_eid
)

select * from final
