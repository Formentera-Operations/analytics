{{
    config(
        materialized='table',
        unique_key='well_performance_monthly_sk',
        cluster_by=['eid', 'production_month'],
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Well Production Monthly
    ==============================

    PURPOSE:
    Monthly production volumes per well-EID, with LOS financial context joined in.
    This is the canonical gold-layer production fact — source-agnostic (ProdView
    today, additional sources can be unioned in at the staging/intermediate layer
    without changing this model's contract).

    GRAIN:
    One row per (eid, production_month) for resolved wells.
    One row per (id_rec_unit, production_month) for unresolved wells (eid IS NULL).
    Multiple ProdView units mapping to the same EID are aggregated into one row.

    EID RESOLUTION (two-step COALESCE, 81.5% match on pvunitcomp units):
    1. Primary:  stg_prodview__units.id_rec = well_360.prodview_unit_id
    2. Fallback: stg_prodview__units.api_10 = well_360.api_10
       (deduplicated to 1 EID per api_10 — prefer operated wells to avoid fan-out)
    ~0.7% of pvunitcomp unit-months are unresolved in current dev build.

    UPSTREAM FILTER:
    stg_prodview__units is filtered to unit_type = 'pvunitcomp' only.
    This excludes facilities, external ins/outs, and meters (~3K units).

    DEPENDENCIES:
    - stg_prodview__daily_allocations
    - stg_prodview__units
    - well_360
    - int_los__well_monthly (ephemeral)
#}

with

-- =============================================================================
-- PRODUCTION SPINE: daily allocations
-- =============================================================================
daily_allocations as (
    select
        id_rec_unit,
        allocation_date,
        allocated_oil_bbl,
        allocated_gas_mcf,
        allocated_water_bbl,
        allocated_ngl_bbl
    from {{ ref('stg_prodview__daily_allocations') }}
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
-- PRODUCTION BY EID-MONTH: join, resolve EID, aggregate to grain
-- Grain: (eid, production_month) for resolved; (id_rec_unit, production_month) for unresolved
-- Multiple units mapping to the same EID are collapsed into one row.
-- =============================================================================
production_with_eid as (
    select
        date_trunc('month', a.allocation_date)::date as production_month,
        -- For unresolved wells only, keep unit ID as the grain discriminator
        coalesce(w1.eid, w2.eid) as eid,
        case
            when coalesce(w1.eid, w2.eid) is null
                then a.id_rec_unit
        end as id_rec_unit,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        -- Volume aggregation (daily → monthly, units per EID per month collapsed)
        sum(a.allocated_oil_bbl) as oil_bbls,
        sum(a.allocated_gas_mcf) as gas_mcf,
        sum(a.allocated_water_bbl) as water_bbls,
        sum(a.allocated_ngl_bbl) as ngl_bbls

    from daily_allocations a
    inner join units u
        on a.id_rec_unit = u.id_rec
    left join well_dim_primary w1
        on u.id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on u.api_10 = w2.pvunit_api_10
    group by
        coalesce(w1.eid, w2.eid),
        case when coalesce(w1.eid, w2.eid) is null then a.id_rec_unit end,
        date_trunc('month', a.allocation_date)::date,
        coalesce(w1.eid, w2.eid) is null
),

-- =============================================================================
-- LOS FINANCIALS: ephemeral intermediate rolled up to well-month
-- =============================================================================
los_monthly as (
    select * from {{ ref('int_los__well_monthly') }}
),

-- =============================================================================
-- FINAL ASSEMBLY: production + LOS join
-- =============================================================================
final as (
    select
        -- surrogate key: use eid for resolved wells, id_rec_unit for unresolved
        {{ dbt_utils.generate_surrogate_key(['coalesce(p.eid, p.id_rec_unit)', 'p.production_month']) }}
            as well_performance_monthly_sk,

        -- grain keys
        p.eid,
        p.id_rec_unit,
        p.production_month,

        -- eid resolution flag
        p.is_eid_unresolved,

        -- production volumes
        coalesce(p.oil_bbls, 0) as oil_bbls,
        coalesce(p.gas_mcf, 0) as gas_mcf,
        coalesce(p.water_bbls, 0) as water_bbls,
        coalesce(p.ngl_bbls, 0) as ngl_bbls,

        -- BOE formula: oil + gas/6
        coalesce(p.oil_bbls, 0) + (coalesce(p.gas_mcf, 0) / 6)
            as gross_boe,

        -- LOS financial context (NULL when no LOS entry for this well-month)
        l.los_month is not null as has_los_entry,
        l.los_revenue,
        l.los_loe,
        l.los_severance_tax,
        l.los_net_income,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from production_with_eid p
    left join los_monthly l
        on
            p.eid = l.eid
            and p.production_month = l.los_month
)

select * from final
