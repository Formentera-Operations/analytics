{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

with contracts as (
    select
        -- identifiers
        --meter_statement_id,
        contract_number,
        --primary_contract_number,
        meter_number,
        --meter_name,
        --facility_name,
        
        -- parties
        --producer_name,
        --processor_name,
        
        -- contract attributes
        --entity_name,
        --counterparty_name,
        basin,
        --play,
        --geographic_area_name,
        business_unit_tag,
        --region_tag,
        --contract_status,
        
        -- dates
        production_date,
        production_month,
        production_year,
        
        -- base volumes
        paystation_mcf,
        allocated_residue_mcf,
        settlement_residue_mcf,
        
        -- shrink metrics
        allocated_shrink_factor,
        settlement_shrink_factor,
        --allocated_shrink_pct,
        --settlement_shrink_pct,
        
        -- allocated yields (bbl/mcf)
        --c2_allocated_yield_bbl_per_mcf,
        --c3_allocated_yield_bbl_per_mcf,
        --ic4_allocated_yield_bbl_per_mcf,
        --nc4_allocated_yield_bbl_per_mcf,
        --c5_plus_allocated_yield_bbl_per_mcf,
        --condensate_allocated_yield_bbl_per_mcf,
        total_allocated_yield_bbl_per_mcf,
        
        -- settlement yields (bbl/mcf)
        --c2_settlement_yield_bbl_per_mcf,
        --c3_settlement_yield_bbl_per_mcf,
        --ic4_settlement_yield_bbl_per_mcf,
        --nc4_settlement_yield_bbl_per_mcf,
        --c5_plus_settlement_yield_bbl_per_mcf,
        --condensate_settlement_yield_bbl_per_mcf,
        total_settlement_yield_bbl_per_mcf,
        
        -- total barrels
        total_allocated_ngl_barrels,
        total_settlement_ngl_barrels,
        
        -- values for reference
        --c2_value_statement,
        --c3_value_statement,
        --ic4_value_statement,
        --nc4_value_statement,
        --c5_plus_value_statement,
        --condensate_value_statement,
        --liquid_value_statement,
        --residue_value_statement,
        statement_net_value,

    from {{ ref('int_aegis__shrink_and_yield') }}
),

aggregated as (
    select
        business_unit_tag AS "Business Unit"
        ,basin AS "Basin"
        ,TO_VARCHAR(production_date::DATE, 'yyyy-mm') AS "Prod YYYY-MM" 
        ,production_month AS "Prod Month"
        ,production_year AS "Prod Year"

        ,count(distinct contract_number) as "Contract Count"
        ,count(distinct meter_number) as "Meter Count"
        ,sum(paystation_mcf) as "Total Paystation MCF"
                
        -- weighted average shrink
        ,sum(allocated_shrink_factor * paystation_mcf) / nullif(sum(paystation_mcf), 0) 
            as "Weighted Average Allocated Shrink Factor"
        ,sum(settlement_shrink_factor * paystation_mcf) / nullif(sum(paystation_mcf), 0) 
            as "Weighted Average Settlement Shrink Factor"
            
        --non-weighted average shrink
        ,avg(allocated_shrink_factor) as "Average Allocated Shrink Factor"
        ,avg(settlement_shrink_factor) as "Average Settlement Shrink Factor"
        
        -- weighted average yields
        ,sum(total_allocated_yield_bbl_per_mcf * paystation_mcf) / nullif(sum(paystation_mcf), 0) 
            as "Weighted Average Total Allocated Yield bbl/mcf"
        ,sum(total_settlement_yield_bbl_per_mcf * paystation_mcf) / nullif(sum(paystation_mcf), 0) 
            as "Weighted Average Total Settlement Yield bbl/mcf"

        --non-weighted average yields
        ,avg(total_allocated_yield_bbl_per_mcf) AS "Average Total Allocated Yield bbl/mcf"
        ,avg(total_settlement_yield_bbl_per_mcf) AS "Average Total Settlement Yield bbl/mcf"
        
        ,sum(total_allocated_ngl_barrels) as "Total Allocated NGL Barrels"
        ,sum(total_settlement_ngl_barrels) as "Total Settlement NGL Barrels"
        ,sum(statement_net_value) as "Total Net Value"


    from contracts
        where business_unit_tag is not null
    group by ALL
        )

select
    "Average Allocated Shrink Factor"
    ,"Average Total Allocated Yield bbl/mcf"
    ,"Average Total Settlement Yield bbl/mcf"
    ,"Average Settlement Shrink Factor"
    ,"Basin"
    ,"Business Unit"
    ,"Contract Count"
    ,"Meter Count"
    ,"Prod Month"
    ,"Prod Year"
    ,"Prod YYYY-MM"
    ,"Total Allocated NGL Barrels"
    ,"Total Net Value"
    ,"Total Paystation MCF"
    ,"Total Settlement NGL Barrels"
    ,"Weighted Average Allocated Shrink Factor"
    ,"Weighted Average Total Allocated Yield bbl/mcf"
    ,"Weighted Average Total Settlement Yield bbl/mcf"
    ,"Weighted Average Settlement Shrink Factor"
from aggregated
        
