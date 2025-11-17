{{
    config(
        materialized='table',
        tags=['aegis', 'intermediate', 'hedging', 'analytics']
    )
}}

with

meter_statements as (

    select * from {{ ref('stg_aegis__meter_statements') }}

),

contracts as (

    select * from {{ ref('stg_aegis__contracts') }}

),

joined as (

    select
        -- identifiers
        ms.meter_statement_id,
        ms.contract_number,
        ms.primary_contract_number,
        ms.meter_number,
        ms.meter_name,
        ms.facility_name,
        
        -- parties
        ms.producer_name,
        ms.processor_name,
        
        -- contract attributes (from contracts table)
        c.entity_name,
        c.counterparty_name,
        c.basin,
        c.play,
        c.geographic_area_name,
        c.business_unit_tag,
        c.region_tag,
        c.contract_status,
        
        -- dates
        ms.accounting_date,
        ms.accounting_year,
        ms.accounting_month,
        ms.production_date,
        ms.production_year,
        ms.production_month,
        
        -- key volumes for calculations
        ms.paystation_mcf,
        ms.paystation_mmbtu,
        ms.allocated_residue_mcf,
        ms.settlement_residue_mcf,
        
        -- ngl allocated volumes (gallons)
        ms.c2_allocated_gallons,
        ms.c3_allocated_gallons,
        ms.ic4_allocated_gallons,
        ms.nc4_allocated_gallons,
        ms.c5_plus_allocated_gallons,
        ms.condensate_allocated_gallons,
        ms.liquid_allocated_gallons,
        
        -- ngl settlement volumes (gallons)
        ms.c2_settlement_gallons,
        ms.c3_settlement_gallons,
        ms.ic4_settlement_gallons,
        ms.nc4_settlement_gallons,
        ms.c5_plus_settlement_gallons,
        ms.condensate_settlement_gallons,
        ms.liquid_settlement_gallons,
        
        -- ngl values for reference
        ms.c2_value_statement,
        ms.c3_value_statement,
        ms.ic4_value_statement,
        ms.nc4_value_statement,
        ms.c5_plus_value_statement,
        ms.condensate_value_statement,
        ms.liquid_value_statement,
        
        -- residue values for reference
        ms.residue_value_statement,
        ms.statement_net_value,
        
        -- metadata
        ms._loaded_at

    from meter_statements ms
    left join contracts c
        on ms.primary_contract_number = c.contract_number

),

calculated as (

    select
        *,
        
        -- ====== SHRINK CALCULATIONS ======
        -- Residue Shrink (residue mcf per wellhead mcf)
        
        case 
            when paystation_mcf > 0 
            then allocated_residue_mcf / paystation_mcf 
            else null 
        end as allocated_shrink_factor,
        
        case 
            when paystation_mcf > 0 
            then settlement_residue_mcf / paystation_mcf 
            else null 
        end as settlement_shrink_factor,
        
        -- Shrink percentage (1 - shrink factor = % lost to shrink)
        case 
            when paystation_mcf > 0 
            then 1 - (allocated_residue_mcf / paystation_mcf)
            else null 
        end as allocated_shrink_pct,
        
        case 
            when paystation_mcf > 0 
            then 1 - (settlement_residue_mcf / paystation_mcf)
            else null 
        end as settlement_shrink_pct,
        
        -- ====== NGL YIELD CALCULATIONS (barrels per mmcf) ======
        -- Allocated Yields (before POP)
        
        case 
            when paystation_mcf > 0 
            then (c2_allocated_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as c2_allocated_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (c3_allocated_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as c3_allocated_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (ic4_allocated_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as ic4_allocated_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (nc4_allocated_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as nc4_allocated_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (c5_plus_allocated_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as c5_plus_allocated_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (condensate_allocated_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as condensate_allocated_yield_bbl_per_mmcf,
        
        -- Settlement Yields (after POP)
        
        case 
            when paystation_mcf > 0 
            then (c2_settlement_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as c2_settlement_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (c3_settlement_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as c3_settlement_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (ic4_settlement_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as ic4_settlement_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (nc4_settlement_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as nc4_settlement_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (c5_plus_settlement_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as c5_plus_settlement_yield_bbl_per_mmcf,
        
        case 
            when paystation_mcf > 0 
            then (condensate_settlement_gallons / 42.0) / (paystation_mcf / 1000.0)
            else null 
        end as condensate_settlement_yield_bbl_per_mmcf,
        
        -- ====== NGL YIELD CALCULATIONS (barrels per mcf) ======
        -- Allocated Yields (before POP) - MCF basis
        
        case 
            when paystation_mcf > 0 
            then (c2_allocated_gallons / 42.0) / paystation_mcf
            else null 
        end as c2_allocated_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (c3_allocated_gallons / 42.0) / paystation_mcf
            else null 
        end as c3_allocated_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (ic4_allocated_gallons / 42.0) / paystation_mcf
            else null 
        end as ic4_allocated_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (nc4_allocated_gallons / 42.0) / paystation_mcf
            else null 
        end as nc4_allocated_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (c5_plus_allocated_gallons / 42.0) / paystation_mcf
            else null 
        end as c5_plus_allocated_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (condensate_allocated_gallons / 42.0) / paystation_mcf
            else null 
        end as condensate_allocated_yield_bbl_per_mcf,
        
        -- Settlement Yields (after POP) - MCF basis
        
        case 
            when paystation_mcf > 0 
            then (c2_settlement_gallons / 42.0) / paystation_mcf
            else null 
        end as c2_settlement_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (c3_settlement_gallons / 42.0) / paystation_mcf
            else null 
        end as c3_settlement_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (ic4_settlement_gallons / 42.0) / paystation_mcf
            else null 
        end as ic4_settlement_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (nc4_settlement_gallons / 42.0) / paystation_mcf
            else null 
        end as nc4_settlement_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (c5_plus_settlement_gallons / 42.0) / paystation_mcf
            else null 
        end as c5_plus_settlement_yield_bbl_per_mcf,
        
        case 
            when paystation_mcf > 0 
            then (condensate_settlement_gallons / 42.0) / paystation_mcf
            else null 
        end as condensate_settlement_yield_bbl_per_mcf

    from joined

),

enhanced as (

    select
        *,
        
        -- Total allocated NGL yield (sum of all components) - MMCF basis
        coalesce(c2_allocated_yield_bbl_per_mmcf, 0) +
        coalesce(c3_allocated_yield_bbl_per_mmcf, 0) +
        coalesce(ic4_allocated_yield_bbl_per_mmcf, 0) +
        coalesce(nc4_allocated_yield_bbl_per_mmcf, 0) +
        coalesce(c5_plus_allocated_yield_bbl_per_mmcf, 0) +
        coalesce(condensate_allocated_yield_bbl_per_mmcf, 0) as total_allocated_yield_bbl_per_mmcf,
        
        -- Total settlement NGL yield (sum of all components) - MMCF basis
        coalesce(c2_settlement_yield_bbl_per_mmcf, 0) +
        coalesce(c3_settlement_yield_bbl_per_mmcf, 0) +
        coalesce(ic4_settlement_yield_bbl_per_mmcf, 0) +
        coalesce(nc4_settlement_yield_bbl_per_mmcf, 0) +
        coalesce(c5_plus_settlement_yield_bbl_per_mmcf, 0) +
        coalesce(condensate_settlement_yield_bbl_per_mmcf, 0) as total_settlement_yield_bbl_per_mmcf,
        
        -- Total allocated NGL yield (sum of all components) - MCF basis
        coalesce(c2_allocated_yield_bbl_per_mcf, 0) +
        coalesce(c3_allocated_yield_bbl_per_mcf, 0) +
        coalesce(ic4_allocated_yield_bbl_per_mcf, 0) +
        coalesce(nc4_allocated_yield_bbl_per_mcf, 0) +
        coalesce(c5_plus_allocated_yield_bbl_per_mcf, 0) +
        coalesce(condensate_allocated_yield_bbl_per_mcf, 0) as total_allocated_yield_bbl_per_mcf,
        
        -- Total settlement NGL yield (sum of all components) - MCF basis
        coalesce(c2_settlement_yield_bbl_per_mcf, 0) +
        coalesce(c3_settlement_yield_bbl_per_mcf, 0) +
        coalesce(ic4_settlement_yield_bbl_per_mcf, 0) +
        coalesce(nc4_settlement_yield_bbl_per_mcf, 0) +
        coalesce(c5_plus_settlement_yield_bbl_per_mcf, 0) +
        coalesce(condensate_settlement_yield_bbl_per_mcf, 0) as total_settlement_yield_bbl_per_mcf,
        
        -- Total allocated NGL barrels
        case 
            when paystation_mcf > 0 
            then (liquid_allocated_gallons / 42.0)
            else null 
        end as total_allocated_ngl_barrels,
        
        -- Total settlement NGL barrels
        case 
            when paystation_mcf > 0 
            then (liquid_settlement_gallons / 42.0)
            else null 
        end as total_settlement_ngl_barrels,
        
        -- Volume normalized to MMCF for weighting
        paystation_mcf / 1000.0 as paystation_mmcf

    from calculated

),

final as (

    select
        -- identifiers
        meter_statement_id,
        contract_number,
        primary_contract_number,
        meter_number,
        meter_name,
        facility_name,
        
        -- parties
        producer_name,
        processor_name,
        
        -- contract attributes
        entity_name,
        counterparty_name,
        basin,
        play,
        geographic_area_name,
        business_unit_tag,
        region_tag,
        contract_status,
        
        -- dates
        accounting_date,
        accounting_year,
        accounting_month,
        production_date,
        production_year,
        production_month,
        
        -- base volumes
        paystation_mcf,
        paystation_mmcf,
        paystation_mmbtu,
        allocated_residue_mcf,
        settlement_residue_mcf,
        
        -- shrink metrics
        allocated_shrink_factor,
        settlement_shrink_factor,
        allocated_shrink_pct,
        settlement_shrink_pct,
        
        -- allocated yields (bbl/mmcf)
        c2_allocated_yield_bbl_per_mmcf,
        c3_allocated_yield_bbl_per_mmcf,
        ic4_allocated_yield_bbl_per_mmcf,
        nc4_allocated_yield_bbl_per_mmcf,
        c5_plus_allocated_yield_bbl_per_mmcf,
        condensate_allocated_yield_bbl_per_mmcf,
        total_allocated_yield_bbl_per_mmcf,
        
        -- settlement yields (bbl/mmcf)
        c2_settlement_yield_bbl_per_mmcf,
        c3_settlement_yield_bbl_per_mmcf,
        ic4_settlement_yield_bbl_per_mmcf,
        nc4_settlement_yield_bbl_per_mmcf,
        c5_plus_settlement_yield_bbl_per_mmcf,
        condensate_settlement_yield_bbl_per_mmcf,
        total_settlement_yield_bbl_per_mmcf,
        
        -- allocated yields (bbl/mcf)
        c2_allocated_yield_bbl_per_mcf,
        c3_allocated_yield_bbl_per_mcf,
        ic4_allocated_yield_bbl_per_mcf,
        nc4_allocated_yield_bbl_per_mcf,
        c5_plus_allocated_yield_bbl_per_mcf,
        condensate_allocated_yield_bbl_per_mcf,
        total_allocated_yield_bbl_per_mcf,
        
        -- settlement yields (bbl/mcf)
        c2_settlement_yield_bbl_per_mcf,
        c3_settlement_yield_bbl_per_mcf,
        ic4_settlement_yield_bbl_per_mcf,
        nc4_settlement_yield_bbl_per_mcf,
        c5_plus_settlement_yield_bbl_per_mcf,
        condensate_settlement_yield_bbl_per_mcf,
        total_settlement_yield_bbl_per_mcf,
        
        -- total barrels
        total_allocated_ngl_barrels,
        total_settlement_ngl_barrels,
        
        -- gallons for reference
        c2_allocated_gallons,
        c3_allocated_gallons,
        ic4_allocated_gallons,
        nc4_allocated_gallons,
        c5_plus_allocated_gallons,
        condensate_allocated_gallons,
        liquid_allocated_gallons,
        c2_settlement_gallons,
        c3_settlement_gallons,
        ic4_settlement_gallons,
        nc4_settlement_gallons,
        c5_plus_settlement_gallons,
        condensate_settlement_gallons,
        liquid_settlement_gallons,
        
        -- values for reference
        c2_value_statement,
        c3_value_statement,
        ic4_value_statement,
        nc4_value_statement,
        c5_plus_value_statement,
        condensate_value_statement,
        liquid_value_statement,
        residue_value_statement,
        statement_net_value,
        
        -- metadata
        _loaded_at

    from enhanced
    where paystation_mcf is not null
        and paystation_mcf > 0  -- only include statements with actual volumes

)

select * from final