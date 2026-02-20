{{
    config(
        materialized='incremental',
        unique_key='distribution_daily_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['eid', 'disposition_date'],
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Well Distribution Daily
    ==============================

    PURPOSE:
    Daily product distribution records for ProdView completions, representing
    where produced volumes went (downstream of allocation). Covers the full
    hydrocarbon component breakdown (C1 through C7+, N2, CO2, H2S) plus
    outlet routing references.

    DISTINCT FROM fct_well_production_daily:
    - Allocation (fct_well_production_daily): how the well PRODUCED — volume
      entering the accounting system, disposition category totals (sales,
      fuel, vent, injected).
    - Distribution (this model): where product WENT — component-level breakdown
      (C1/C2/C3+) with outlet/route context. The allocation's disposition columns
      are category summaries; this model adds NGL component composition detail
      not available anywhere else in the mart.

    GRAIN:
    One row per disposition record (PVT_PVUNITDISPMONTHDAY.IDREC).

    EID RESOLUTION (1-hop — pvUnitDisp has unit_id directly):
    disposition.unit_id → well_360.prodview_unit_id
    NOTE: 100% EID resolution confirmed — all 24.7M records have a unit_id
    that maps directly to well_360.prodview_unit_id. No fallback needed.

    ROWS: ~24.7M as of Sprint 10.
    Incremental watermark: _fivetran_synced from stg_prodview__daily_dispositions.

    DEPENDENCIES:
    - stg_prodview__daily_dispositions
    - well_360
#}

with

-- =============================================================================
-- DISPOSITIONS: source records with incremental filter
-- =============================================================================
dispositions as (
    select
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        disposition_date,
        disposition_year,
        disposition_month,
        day_of_month,

        -- unit and completion references (unit_id enables 1-hop EID)
        unit_id,
        completion_id,
        outlet_send_id,
        disposition_unit_id,
        reporting_contact_interval_id,

        -- total fluid volumes
        hcliq_volume_bbl,
        hcliq_gas_equivalent_mcf,
        gas_volume_mcf,
        water_volume_bbl,
        sand_volume_bbl,

        -- component volumes — C1 (Methane)
        c1_liquid_volume_bbl,
        c1_gas_equivalent_mcf,
        c1_gas_volume_mcf,

        -- C2 (Ethane)
        c2_liquid_volume_bbl,
        c2_gas_equivalent_mcf,
        c2_gas_volume_mcf,

        -- C3 (Propane)
        c3_liquid_volume_bbl,
        c3_gas_equivalent_mcf,
        c3_gas_volume_mcf,

        -- iC4 (Iso-butane)
        ic4_liquid_volume_bbl,
        ic4_gas_equivalent_mcf,
        ic4_gas_volume_mcf,

        -- nC4 (Normal butane)
        nc4_liquid_volume_bbl,
        nc4_gas_equivalent_mcf,
        nc4_gas_volume_mcf,

        -- iC5 (Iso-pentane)
        ic5_liquid_volume_bbl,
        ic5_gas_equivalent_mcf,
        ic5_gas_volume_mcf,

        -- nC5 (Normal pentane)
        nc5_liquid_volume_bbl,
        nc5_gas_equivalent_mcf,
        nc5_gas_volume_mcf,

        -- C6 (Hexanes)
        c6_liquid_volume_bbl,
        c6_gas_equivalent_mcf,
        c6_gas_volume_mcf,

        -- C7+ (Heptanes plus)
        c7_liquid_volume_bbl,
        c7_gas_equivalent_mcf,
        c7_gas_volume_mcf,

        -- non-hydrocarbon components
        n2_liquid_volume_bbl,
        n2_gas_equivalent_mcf,
        n2_gas_volume_mcf,
        co2_liquid_volume_bbl,
        co2_gas_equivalent_mcf,
        co2_gas_volume_mcf,
        h2s_liquid_volume_bbl,
        h2s_gas_equivalent_mcf,
        h2s_gas_volume_mcf,
        other_components_liquid_volume_bbl,
        other_components_gas_equivalent_mcf,
        other_components_gas_volume_mcf,

        -- heat content
        heat_content_mmbtu,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced

    from {{ ref('stg_prodview__daily_dispositions') }}
    {% if is_incremental() %}
        where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}
),

-- =============================================================================
-- EID RESOLUTION: 1-hop via unit_id → well_360.prodview_unit_id
-- 100% match rate confirmed — no fallback needed
-- =============================================================================
well_dim as (
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

-- =============================================================================
-- FINAL ASSEMBLY
-- =============================================================================
final as (
    select  -- noqa: ST06
        {{ dbt_utils.generate_surrogate_key(['d.id_rec']) }}
            as distribution_daily_sk,

        -- grain key
        d.id_rec,
        d.id_rec_parent,
        d.id_flownet,

        -- EID resolution
        w.eid,
        w.eid is null as is_eid_unresolved,

        -- entity references
        d.unit_id,
        d.completion_id,
        d.outlet_send_id,
        d.disposition_unit_id,
        d.reporting_contact_interval_id,

        -- dates
        d.disposition_date,
        d.disposition_year,
        d.disposition_month,
        d.day_of_month,

        -- total fluid volumes
        d.hcliq_volume_bbl,
        d.hcliq_gas_equivalent_mcf,
        d.gas_volume_mcf,
        d.water_volume_bbl,
        d.sand_volume_bbl,

        -- NGL component breakdown — C1 through C7+
        d.c1_liquid_volume_bbl,
        d.c1_gas_equivalent_mcf,
        d.c1_gas_volume_mcf,
        d.c2_liquid_volume_bbl,
        d.c2_gas_equivalent_mcf,
        d.c2_gas_volume_mcf,
        d.c3_liquid_volume_bbl,
        d.c3_gas_equivalent_mcf,
        d.c3_gas_volume_mcf,
        d.ic4_liquid_volume_bbl,
        d.ic4_gas_equivalent_mcf,
        d.ic4_gas_volume_mcf,
        d.nc4_liquid_volume_bbl,
        d.nc4_gas_equivalent_mcf,
        d.nc4_gas_volume_mcf,
        d.ic5_liquid_volume_bbl,
        d.ic5_gas_equivalent_mcf,
        d.ic5_gas_volume_mcf,
        d.nc5_liquid_volume_bbl,
        d.nc5_gas_equivalent_mcf,
        d.nc5_gas_volume_mcf,
        d.c6_liquid_volume_bbl,
        d.c6_gas_equivalent_mcf,
        d.c6_gas_volume_mcf,
        d.c7_liquid_volume_bbl,
        d.c7_gas_equivalent_mcf,
        d.c7_gas_volume_mcf,

        -- non-hydrocarbon components
        d.n2_liquid_volume_bbl,
        d.n2_gas_equivalent_mcf,
        d.n2_gas_volume_mcf,
        d.co2_liquid_volume_bbl,
        d.co2_gas_equivalent_mcf,
        d.co2_gas_volume_mcf,
        d.h2s_liquid_volume_bbl,
        d.h2s_gas_equivalent_mcf,
        d.h2s_gas_volume_mcf,
        d.other_components_liquid_volume_bbl,
        d.other_components_gas_equivalent_mcf,
        d.other_components_gas_volume_mcf,

        -- heat content
        d.heat_content_mmbtu,

        -- audit
        d.created_at_utc,
        d.modified_at_utc,
        d._fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from dispositions d
    left join well_dim w
        on d.unit_id = w.pvunit_id_rec
)

select * from final
