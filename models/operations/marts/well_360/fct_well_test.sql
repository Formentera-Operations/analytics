{{
    config(
        materialized='table',
        unique_key='well_test_sk',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Well Test
    ==============================

    PURPOSE:
    Per-event production test fact for ProdView completions. Each row represents
    one production test (PI test, injection ratio test, etc.) with measured
    volumes, calculated rates, quality ratios, and change-from-prior comparisons.
    Enables PI trending, GOR/WOR monitoring, and test-over-test decline analysis.

    GRAIN:
    One row per production test record (id_rec). Tests are infrequent events
    (typically monthly or quarterly per completion), so volume is low.

    EID RESOLUTION (three-hop via completions → unit → well_360):
    1. Primary:  test.id_rec_parent → completions.id_rec
                 → completions.id_rec_parent (unit ID)
                 → well_360.prodview_unit_id
    2. Fallback: completions.api_10 → well_360.api_10
                 (deduplicated to 1 EID per api_10 — prefer operated wells)

    INVALID TESTS:
    Rows with invalid_test = 'Y' are retained with the flag set — do NOT filter
    them out. Consumers that want only valid tests should filter WHERE
    coalesce(invalid_test, 'N') != 'Y'.

    DEPENDENCIES:
    - stg_prodview__production_tests
    - stg_prodview__completions (for EID resolution)
    - well_360
#}

with

-- =============================================================================
-- PRODUCTION TESTS: mart-relevant columns from staging
-- =============================================================================
tests as (
    select
        id_rec,
        id_rec_parent,   -- completion ID (FK to stg_prodview__completions.id_rec)
        test_type,
        effective_date,
        test_hours,
        invalid_test,
        tested_by,
        choke_size_64ths,
        note,

        -- test volumes (amounts measured during the test period)
        test_oil_condensate_volume_bbl,
        test_gas_volume_mcf,
        test_water_volume_bbl,
        test_total_fluid_volume_bbl,
        test_sand_volume_bbl,

        -- test rates (key analytical outputs — normalized to per-day)
        oil_cond_rate_bbl_per_day,
        produced_gas_rate_mcf_per_day,
        water_rate_bbl_per_day,
        total_fluid_rate_bbl_per_day,
        total_gas_rate_mcf_per_day,
        sand_rate_bbl_per_day,
        lift_gas_rate_mcf_per_day,

        -- quality ratios
        gas_oil_ratio_mcf_per_bbl,
        condensate_gas_ratio_bbl_per_mcf,
        water_gas_ratio_bbl_per_mcf,
        total_bsw_pct,
        total_sand_cut_pct,

        -- pressures
        pressure_wellhead_psi,
        pressure_bh_psi,
        casing_pressure_psi,
        pressure_prod_sep_psi,
        pressure_test_separator_psi,
        shut_in_wellhead_pressure_psi,
        flowing_wellhead_pressure_psi,

        -- temperatures
        temperature_well_head_f,
        temperature_bottom_hole_f,
        temperature_production_separator_f,
        temperature_test_separator_f,

        -- change from prior test (computed by ProdView at staging time)
        change_in_oil_emulsion_rate_bbl_per_day,
        pct_change_in_oil_emulsion_rate_pct,
        change_in_gas_rate_mcf_per_day,
        pct_change_in_gas_rate_pct,
        change_in_water_rate_bbl_per_day,
        pct_change_in_water_rate_pct,
        change_in_gor_mcf_per_bbl,
        pct_change_in_gor_pct,
        change_in_cgr_bbl_per_mcf,
        pct_change_in_cgr_pct,
        change_in_wgr_bbl_per_mcf,
        pct_change_in_wgr_pct,
        change_in_bsw_pct,
        pct_change_in_bsw_pct,
        reason_for_variance,

        -- purpose flags
        allocation_flag,
        deliverability_flag,
        regulatory_flag,
        data_source,

        -- fluid quality
        gas_specific_gravity,
        condensate_gravity_api,
        oil_emul_bsw_pct,
        gas_in_solution_factor_mcf_per_bbl,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced

    from {{ ref('stg_prodview__production_tests') }}
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
-- TESTS WITH EID: join completions, resolve EID
-- =============================================================================
tests_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        t.id_rec,
        t.id_rec_parent as id_rec_comp,
        t.test_type,
        t.effective_date,
        t.test_hours,
        t.invalid_test,
        t.tested_by,
        t.choke_size_64ths,
        t.note,
        t.test_oil_condensate_volume_bbl,
        t.test_gas_volume_mcf,
        t.test_water_volume_bbl,
        t.test_total_fluid_volume_bbl,
        t.test_sand_volume_bbl,
        t.oil_cond_rate_bbl_per_day,
        t.produced_gas_rate_mcf_per_day,
        t.water_rate_bbl_per_day,
        t.total_fluid_rate_bbl_per_day,
        t.total_gas_rate_mcf_per_day,
        t.sand_rate_bbl_per_day,
        t.lift_gas_rate_mcf_per_day,
        t.gas_oil_ratio_mcf_per_bbl,
        t.condensate_gas_ratio_bbl_per_mcf,
        t.water_gas_ratio_bbl_per_mcf,
        t.total_bsw_pct,
        t.total_sand_cut_pct,
        t.pressure_wellhead_psi,
        t.pressure_bh_psi,
        t.casing_pressure_psi,
        t.pressure_prod_sep_psi,
        t.pressure_test_separator_psi,
        t.shut_in_wellhead_pressure_psi,
        t.flowing_wellhead_pressure_psi,
        t.temperature_well_head_f,
        t.temperature_bottom_hole_f,
        t.temperature_production_separator_f,
        t.temperature_test_separator_f,
        t.change_in_oil_emulsion_rate_bbl_per_day,
        t.pct_change_in_oil_emulsion_rate_pct,
        t.change_in_gas_rate_mcf_per_day,
        t.pct_change_in_gas_rate_pct,
        t.change_in_water_rate_bbl_per_day,
        t.pct_change_in_water_rate_pct,
        t.change_in_gor_mcf_per_bbl,
        t.pct_change_in_gor_pct,
        t.change_in_cgr_bbl_per_mcf,
        t.pct_change_in_cgr_pct,
        t.change_in_wgr_bbl_per_mcf,
        t.pct_change_in_wgr_pct,
        t.change_in_bsw_pct,
        t.pct_change_in_bsw_pct,
        t.reason_for_variance,
        t.allocation_flag,
        t.deliverability_flag,
        t.regulatory_flag,
        t.data_source,
        t.gas_specific_gravity,
        t.condensate_gravity_api,
        t.oil_emul_bsw_pct,
        t.gas_in_solution_factor_mcf_per_bbl,
        t.created_at_utc,
        t.modified_at_utc,
        t._fivetran_synced

    from tests t
    left join completions c
        on t.id_rec_parent = c.completion_id_rec
    left join well_dim_primary w1
        on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on c.completion_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY: surrogate key + derived BOE rate
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as well_test_sk,

        -- grain key + EID
        id_rec,
        id_rec_comp,
        eid,
        is_eid_unresolved,

        -- test metadata
        test_type,
        effective_date,
        test_hours,
        invalid_test,
        tested_by,
        choke_size_64ths,
        note,

        -- test volumes
        test_oil_condensate_volume_bbl,
        test_gas_volume_mcf,
        test_water_volume_bbl,
        test_total_fluid_volume_bbl,
        test_sand_volume_bbl,

        -- test rates
        oil_cond_rate_bbl_per_day,
        produced_gas_rate_mcf_per_day,
        water_rate_bbl_per_day,
        total_fluid_rate_bbl_per_day,
        total_gas_rate_mcf_per_day,
        sand_rate_bbl_per_day,
        lift_gas_rate_mcf_per_day,

        -- derived BOE rate (oil + gas/6)
        coalesce(oil_cond_rate_bbl_per_day, 0) + (coalesce(produced_gas_rate_mcf_per_day, 0) / 6)
            as gross_boe_rate_bbl_per_day,

        -- quality ratios
        gas_oil_ratio_mcf_per_bbl,
        condensate_gas_ratio_bbl_per_mcf,
        water_gas_ratio_bbl_per_mcf,
        total_bsw_pct,
        total_sand_cut_pct,

        -- pressures
        pressure_wellhead_psi,
        pressure_bh_psi,
        casing_pressure_psi,
        pressure_prod_sep_psi,
        pressure_test_separator_psi,
        shut_in_wellhead_pressure_psi,
        flowing_wellhead_pressure_psi,

        -- temperatures
        temperature_well_head_f,
        temperature_bottom_hole_f,
        temperature_production_separator_f,
        temperature_test_separator_f,

        -- change from prior test
        change_in_oil_emulsion_rate_bbl_per_day,
        pct_change_in_oil_emulsion_rate_pct,
        change_in_gas_rate_mcf_per_day,
        pct_change_in_gas_rate_pct,
        change_in_water_rate_bbl_per_day,
        pct_change_in_water_rate_pct,
        change_in_gor_mcf_per_bbl,
        pct_change_in_gor_pct,
        change_in_cgr_bbl_per_mcf,
        pct_change_in_cgr_pct,
        change_in_wgr_bbl_per_mcf,
        pct_change_in_wgr_pct,
        change_in_bsw_pct,
        pct_change_in_bsw_pct,
        reason_for_variance,

        -- purpose flags
        allocation_flag,
        deliverability_flag,
        regulatory_flag,
        data_source,

        -- fluid quality
        gas_specific_gravity,
        condensate_gravity_api,
        oil_emul_bsw_pct,
        gas_in_solution_factor_mcf_per_bbl,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from tests_with_eid
)

select * from final
