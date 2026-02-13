{{
    config(
        materialized='view',
        tags=['well_360', 'source_prep', 'production']
    )
}}

{#
    First Production Date from Daily Allocations
    =============================================

    Derives the TRUE first production date by finding the earliest date
    with non-zero hydrocarbon production in daily allocation data.

    This is more accurate than WellView/Enverus dates which may reflect
    acquisition date rather than actual first production.

    Source: stg_prodview__daily_allocations
    Grain: One row per unit record ID
#}

with daily_production as (
    select
        id_rec_unit as unit_id,
        allocation_date as production_date,
        -- Sum all hydrocarbon volumes
        coalesce(allocated_oil_bbl, 0)
        + coalesce(allocated_condensate_bbl, 0)
        + coalesce(allocated_ngl_bbl, 0) as total_oil_bbl,
        coalesce(allocated_gas_mcf, 0) as total_gas_mcf
    from {{ ref('stg_prodview__daily_allocations') }}
    where allocation_date is not null
),

-- Find first date with non-zero hydrocarbon production
first_hc_production as (
    select
        unit_id,
        min(production_date) as first_hc_production_date
    from daily_production
    where total_oil_bbl > 0 or total_gas_mcf > 0
    group by unit_id
),

-- Also get first date with any production record (even if zero)
first_any_record as (
    select
        unit_id,
        min(production_date) as first_allocation_date
    from daily_production
    group by unit_id
)

select
    fhc.first_hc_production_date,  -- First date with actual HC production
    far.first_allocation_date,     -- First date with any allocation record
    coalesce(fhc.unit_id, far.unit_id) as unit_id
from first_hc_production fhc
full outer join first_any_record far on fhc.unit_id = far.unit_id
