{{
    config(
        materialized='table',
        unique_key='facility_monthly_sk',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Facility Monthly
    ==============================

    PURPOSE:
    Monthly production/injection balance at the ProdView facility level.
    A facility (pvFacility) is a multi-well aggregate entity — a battery, tank
    battery, lease aggregate, or other gathering group. This fact covers the
    full volume balance for each facility per month: production inputs,
    gathered completion volumes, receipts in, injected volumes, consumed
    volumes, dispositions out, inventory changes, and balance status flags.

    This is the primary mart target for facility-level balance analytics per
    the ProdView entity model documentation.

    GRAIN:
    One row per monthly calc record (id_rec from PVT_PVFACILITYMONTHCALC).
    Each facility has one row per calendar month in which it is active.
    Unique on (id_rec_facility, facility_month).

    FACILITY vs. WELL-LEVEL GRAIN:
    pvFacility is a SEPARATE ROOT ENTITY from pvUnit — do NOT join using the
    standard pvUnitComp → completion → unit → well_360 EID chain. Facilities
    are multi-well aggregates; they do not map 1:1 to EIDs.

    EID RESOLUTION:
    Not applied in this model. pvFacility.IDPa carries the facility EID but
    PVT_PVFACILITY is not yet registered as a dbt source and no
    stg_prodview__facilities staging model exists. Use `id_rec_facility` to
    join to a future facility dimension when available.

    VOLUMES:
    All volumes sourced from ProdView's calculated monthly rollup
    (PVT_PVFACILITYMONTHCALC). This is a system-generated calc table —
    values are the authoritative allocation outputs, not raw gauge readings.

    DEPENDENCIES:
    - stg_prodview__facility_monthly_volumes  (sole source — no EID joins)
#}

with

-- =============================================================================
-- FACILITY MONTHLY VOLUMES: all analytical columns from staging
-- =============================================================================
facility_monthly as (
    select
        id_rec,
        id_rec_parent as id_rec_facility,
        id_flownet,

        -- period dates + clean month date
        period_start_date::date as period_start_date,
        period_end_date::date as period_end_date,
        date_trunc('month', period_start_date)::date as facility_month,
        calculation_year::int as calculation_year,
        calculation_month::int as calculation_month,

        -- production volumes
        produced_hcliq_bbl,
        produced_gas_mcf,
        produced_gas_plus_gas_eq_mcf,
        produced_water_bbl,
        produced_sand_bbl,

        -- gathered completion volumes
        gathered_comp_hcliq_bbl,
        gathered_comp_gas_mcf,
        gathered_comp_gas_plus_gas_eq_mcf,
        gathered_comp_water_bbl,
        gathered_comp_sand_bbl,

        -- proration factors
        proration_factor_hcliq,
        proration_factor_gas,
        proration_factor_gas_plus_gas_eq,
        proration_factor_water,
        proration_factor_sand,

        -- volume balance
        volume_balance_hcliq_bbl,
        volume_balance_gas_mcf,
        volume_balance_gas_plus_gas_eq_mcf,
        volume_balance_water_bbl,
        volume_balance_sand_bbl,

        -- balance status flags
        all_products_balanced,
        hcliq_balanced,
        gas_balanced,
        gas_plus_gas_eq_balanced,
        water_balanced,
        sand_balanced,

        -- ins - recovered load/lift volumes
        recovered_load_hcliq_bbl,
        recovered_lift_gas_mcf,
        recovered_lift_gas_plus_gas_eq_mcf,
        recovered_load_water_bbl,
        recovered_load_sand_bbl,

        -- ins - other receipts
        receipts_in_hcliq_bbl,
        receipts_in_gas_mcf,
        receipts_in_gas_plus_gas_eq_mcf,
        receipts_in_water_bbl,
        receipts_in_sand_bbl,

        -- outs - consumed volumes
        consumed_hcliq_bbl,
        consumed_gas_mcf,
        consumed_gas_plus_gas_eq_mcf,
        consumed_water_bbl,
        consumed_sand_bbl,

        -- outs - injected load/lift volumes
        injected_load_hcliq_bbl,
        injected_lift_gas_mcf,
        injected_lift_gas_plus_gas_eq_mcf,
        injected_load_water_bbl,
        injected_sand_bbl,

        -- outs - other dispositions
        dispositions_out_hcliq_bbl,
        dispositions_out_gas_mcf,
        dispositions_out_gas_plus_gas_eq_mcf,
        dispositions_out_water_bbl,
        dispositions_out_sand_bbl,

        -- opening remaining load/lift
        opening_remaining_load_hcliq_bbl,
        opening_remaining_lift_gas_mcf,
        opening_remaining_lift_gas_plus_gas_eq_mcf,
        opening_remaining_load_water_bbl,
        opening_remaining_sand_bbl,

        -- closing remaining load/lift
        closing_remaining_load_hcliq_bbl,
        closing_remaining_lift_gas_mcf,
        closing_remaining_lift_gas_plus_gas_eq_mcf,
        closing_remaining_load_water_bbl,
        closing_remaining_sand_bbl,

        -- inventory - opening
        opening_inventory_hcliq_bbl,
        opening_inventory_gas_equivalent_hcliq_mcf,
        opening_inventory_water_bbl,
        opening_inventory_sand_bbl,

        -- inventory - closing
        closing_inventory_hcliq_bbl,
        closing_inventory_gas_equiv_hcliq_mcf,
        closing_inventory_water_bbl,
        closing_inventory_sand_bbl,

        -- inventory - change
        change_in_inventory_hcliq_bbl,
        change_in_inventory_gas_equivalent_hcliq_mcf,
        change_in_inventory_water_bbl,
        change_in_inventory_sand_bbl,

        -- other volumes
        stv_gas_mcf,

        -- propane and butane (remain in m3 — no conversion macro available)
        produced_propane_m3,
        produced_butane_m3,
        receipts_in_propane_m3,
        receipts_in_butane_m3,
        dispositions_out_propane_m3,
        dispositions_out_butane_m3,
        opening_inventory_propane_m3,
        opening_inventory_butane_m3,
        closing_inventory_propane_m3,
        closing_inventory_butane_m3,
        change_in_inventory_propane_m3,
        change_in_inventory_butane_m3,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced

    from {{ ref('stg_prodview__facility_monthly_volumes') }}
),

-- =============================================================================
-- FINAL ASSEMBLY: surrogate key + derived BOE
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }}
            as facility_monthly_sk,

        -- grain keys
        id_rec,
        id_rec_facility,
        id_flownet,
        facility_month,

        -- period dates + calendar components
        period_start_date,
        period_end_date,
        calculation_year,
        calculation_month,

        -- production volumes
        produced_hcliq_bbl,
        produced_gas_mcf,
        produced_gas_plus_gas_eq_mcf,
        produced_water_bbl,
        produced_sand_bbl,

        -- derived facility BOE (produced hcliq + produced gas / 6)
        coalesce(produced_hcliq_bbl, 0) + (coalesce(produced_gas_mcf, 0) / 6)
            as produced_gross_boe,

        -- gathered completion volumes
        gathered_comp_hcliq_bbl,
        gathered_comp_gas_mcf,
        gathered_comp_gas_plus_gas_eq_mcf,
        gathered_comp_water_bbl,
        gathered_comp_sand_bbl,

        -- proration factors
        proration_factor_hcliq,
        proration_factor_gas,
        proration_factor_gas_plus_gas_eq,
        proration_factor_water,
        proration_factor_sand,

        -- volume balance
        volume_balance_hcliq_bbl,
        volume_balance_gas_mcf,
        volume_balance_gas_plus_gas_eq_mcf,
        volume_balance_water_bbl,
        volume_balance_sand_bbl,

        -- balance status flags
        all_products_balanced,
        hcliq_balanced,
        gas_balanced,
        gas_plus_gas_eq_balanced,
        water_balanced,
        sand_balanced,

        -- ins - recovered load/lift volumes
        recovered_load_hcliq_bbl,
        recovered_lift_gas_mcf,
        recovered_lift_gas_plus_gas_eq_mcf,
        recovered_load_water_bbl,
        recovered_load_sand_bbl,

        -- ins - other receipts
        receipts_in_hcliq_bbl,
        receipts_in_gas_mcf,
        receipts_in_gas_plus_gas_eq_mcf,
        receipts_in_water_bbl,
        receipts_in_sand_bbl,

        -- outs - consumed volumes
        consumed_hcliq_bbl,
        consumed_gas_mcf,
        consumed_gas_plus_gas_eq_mcf,
        consumed_water_bbl,
        consumed_sand_bbl,

        -- outs - injected load/lift volumes
        injected_load_hcliq_bbl,
        injected_lift_gas_mcf,
        injected_lift_gas_plus_gas_eq_mcf,
        injected_load_water_bbl,
        injected_sand_bbl,

        -- outs - other dispositions
        dispositions_out_hcliq_bbl,
        dispositions_out_gas_mcf,
        dispositions_out_gas_plus_gas_eq_mcf,
        dispositions_out_water_bbl,
        dispositions_out_sand_bbl,

        -- opening remaining load/lift
        opening_remaining_load_hcliq_bbl,
        opening_remaining_lift_gas_mcf,
        opening_remaining_lift_gas_plus_gas_eq_mcf,
        opening_remaining_load_water_bbl,
        opening_remaining_sand_bbl,

        -- closing remaining load/lift
        closing_remaining_load_hcliq_bbl,
        closing_remaining_lift_gas_mcf,
        closing_remaining_lift_gas_plus_gas_eq_mcf,
        closing_remaining_load_water_bbl,
        closing_remaining_sand_bbl,

        -- inventory - opening
        opening_inventory_hcliq_bbl,
        opening_inventory_gas_equivalent_hcliq_mcf,
        opening_inventory_water_bbl,
        opening_inventory_sand_bbl,

        -- inventory - closing
        closing_inventory_hcliq_bbl,
        closing_inventory_gas_equiv_hcliq_mcf,
        closing_inventory_water_bbl,
        closing_inventory_sand_bbl,

        -- inventory - change
        change_in_inventory_hcliq_bbl,
        change_in_inventory_gas_equivalent_hcliq_mcf,
        change_in_inventory_water_bbl,
        change_in_inventory_sand_bbl,

        -- other volumes
        stv_gas_mcf,

        -- propane and butane (m3 — conversion not available via project macros)
        produced_propane_m3,
        produced_butane_m3,
        receipts_in_propane_m3,
        receipts_in_butane_m3,
        dispositions_out_propane_m3,
        dispositions_out_butane_m3,
        opening_inventory_propane_m3,
        opening_inventory_butane_m3,
        closing_inventory_propane_m3,
        closing_inventory_butane_m3,
        change_in_inventory_propane_m3,
        change_in_inventory_butane_m3,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from facility_monthly
)

select * from final
