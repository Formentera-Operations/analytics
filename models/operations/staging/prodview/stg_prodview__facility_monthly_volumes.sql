{{ config(
    materialized='view',
    tags=['prodview', 'facilities', 'production', 'monthly', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVFACILITYMONTHCALC') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as facility_calculation_id,
        idrecparent as parent_facility_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttmstart as period_start_date,
        dttmend as period_end_date,
        year as calculation_year,
        month as calculation_month,
        
        -- Production volumes (converted to US units)
        volprodhcliq / 0.158987294928 as produced_hcliq_bbl,
        volprodgas / 28.316846592 as produced_gas_mcf,
        volprodgasplusgaseq / 28.316846592 as produced_gas_plus_gas_eq_mcf,
        volprodwater / 0.158987294928 as produced_water_bbl,
        volprodsand / 0.158987294928 as produced_sand_bbl,
        
        -- Gathered completion volumes / Estimate (converted to US units)
        volnewprodgathhcliq / 0.158987294928 as gathered_comp_hcliq_bbl,
        volnewprodgathgas / 28.316846592 as gathered_comp_gas_mcf,
        volnewprodgathgasplusgaseq / 28.316846592 as gathered_comp_gas_plus_gas_eq_mcf,
        volnewprodgathwater / 0.158987294928 as gathered_comp_water_bbl,
        volnewprodgathsand / 0.158987294928 as gathered_comp_sand_bbl,
        
        -- Balance factors (unitless ratios)
        balfacthcliq as proration_factor_hcliq,
        balfactgas as proration_factor_gas,
        balfactgasplusgaseq as proration_factor_gas_plus_gas_eq,
        balfactwater as proration_factor_water,
        balfactsand as proration_factor_sand,
        
        -- Volume balance (converted to US units)
        volbalhcliq / 0.158987294928 as volume_balance_hcliq_bbl,
        volbalgas / 28.316846592 as volume_balance_gas_mcf,
        volbalgasplusgaseq / 28.316846592 as volume_balance_gas_plus_gas_eq_mcf,
        volbalwater / 0.158987294928 as volume_balance_water_bbl,
        volbalsand / 0.158987294928 as volume_balance_sand_bbl,
        
        -- Balance status flags
        balanced as all_products_balanced,
        balhcliq as hcliq_balanced,
        balgas as gas_balanced,
        balgasplusgaseq as gas_plus_gas_eq_balanced,
        balwater as water_balanced,
        balsand as sand_balanced,
        
        -- Ins - Recovered volumes (converted to US units)
        volinrecovhcliq / 0.158987294928 as recovered_load_hcliq_bbl,
        volinrecovgas / 28.316846592 as recovered_lift_gas_mcf,
        volinrecovgasplusgaseq / 28.316846592 as recovered_lift_gas_plus_gas_eq_mcf,
        volinrecovwater / 0.158987294928 as recovered_load_water_bbl,
        volinrecovsand / 0.158987294928 as recovered_load_sand_bbl,
        
        -- Ins - Other receipts (converted to US units)
        volinotherhcliq / 0.158987294928 as receipts_in_hcliq_bbl,
        volinothergas / 28.316846592 as receipts_in_gas_mcf,
        volinothergasplusgaseq / 28.316846592 as receipts_in_gas_plus_gas_eq_mcf,
        volinotherwater / 0.158987294928 as receipts_in_water_bbl,
        volinothersand / 0.158987294928 as receipts_in_sand_bbl,
        
        -- Outs - Consumed volumes (converted to US units)
        voloutconsumehcliq / 0.158987294928 as consumed_hcliq_bbl,
        voloutconsumegas / 28.316846592 as consumed_gas_mcf,
        voloutconsumegasplusgaseq / 28.316846592 as consumed_gas_plus_gas_eq_mcf,
        voloutconsumewater / 0.158987294928 as consumed_water_bbl,
        voloutconsumesand / 0.158987294928 as consumed_sand_bbl,
        
        -- Outs - Injected volumes (converted to US units)
        voloutinjectrecovhcliq / 0.158987294928 as injected_load_hcliq_bbl,
        voloutinjectrecovgas / 28.316846592 as injected_lift_gas_mcf,
        voloutinjectrecovgasplusgaseq / 28.316846592 as injected_lift_gas_plus_gas_eq_mcf,
        voloutinjectrecovwater / 0.158987294928 as injected_load_water_bbl,
        voloutinjectrecovsand / 0.158987294928 as injected_sand_bbl,
        
        -- Outs - Other dispositions (converted to US units)
        voloutotherhcliq / 0.158987294928 as dispositions_out_hcliq_bbl,
        voloutothergas / 28.316846592 as dispositions_out_gas_mcf,
        voloutothergasplusgaseq / 28.316846592 as dispositions_out_gas_plus_gas_eq_mcf,
        voloutotherwater / 0.158987294928 as dispositions_out_water_bbl,
        voloutothersand / 0.158987294928 as dispositions_out_sand_bbl,
        
        -- Load - Opening remaining volumes (converted to US units)
        volstartremainrecovhcliq / 0.158987294928 as opening_remaining_load_hcliq_bbl,
        volstartremainrecovgas / 28.316846592 as opening_remaining_lift_gas_mcf,
        volstartremainrecovgasplusgeq / 28.316846592 as opening_remaining_lift_gas_plus_gas_eq_mcf,
        volstartremainrecovwater / 0.158987294928 as opening_remaining_load_water_bbl,
        volstartremainrecovsand / 0.158987294928 as opening_remaining_sand_bbl,
        
        -- Load - Closing remaining volumes (converted to US units)
        volendremainrecovhcliq / 0.158987294928 as closing_remaining_load_hcliq_bbl,
        volendremainrecovgas / 28.316846592 as closing_remaining_lift_gas_mcf,
        volendremainrecovgasplusgeq / 28.316846592 as closing_remaining_lift_gas_plus_gas_eq_mcf,
        volendremainrecovwater / 0.158987294928 as closing_remaining_load_water_bbl,
        volendremainrecovsand / 0.158987294928 as closing_remaining_sand_bbl,
        
        -- Inventory - Opening volumes (converted to US units)
        volstartinvhcliq / 0.158987294928 as opening_inventory_hcliq_bbl,
        volstartinvhcliqgaseq / 28.316846592 as opening_inventory_gas_equivalent_hcliq_mcf,
        volstartinvwater / 0.158987294928 as opening_inventory_water_bbl,
        volstartinvsand / 0.158987294928 as opening_inventory_sand_bbl,
        
        -- Inventory - Closing volumes (converted to US units)
        volendinvhcliq / 0.158987294928 as closing_inventory_hcliq_bbl,
        volendinvhcliqgaseq / 28.316846592 as closing_inventory_gas_equiv_hcliq_mcf,
        volendinvwater / 0.158987294928 as closing_inventory_water_bbl,
        volendinvsand / 0.158987294928 as closing_inventory_sand_bbl,
        
        -- Inventory - Change volumes (converted to US units)
        volchginvhcliq / 0.158987294928 as change_in_inventory_hcliq_bbl,
        volchginvhcliqgaseq / 28.316846592 as change_in_inventory_gas_equivalent_hcliq_mcf,
        volchginvwater / 0.158987294928 as change_in_inventory_water_bbl,
        volchginvsand / 0.158987294928 as change_in_inventory_sand_bbl,
        
        -- Other volumes
        volstvgas / 28.316846592 as stv_gas_mcf,
        
        -- Propane and butane volumes (remain in cubic meters)
        volprodpropane as produced_propane_m3,
        volprodbutane as produced_butane_m3,
        volinotherpropane as receipts_in_propane_m3,
        volinotherbutane as receipts_in_butane_m3,
        voloutotherpropane as dispositions_out_propane_m3,
        voloutotherbutane as dispositions_out_butane_m3,
        volstartpropane as opening_inventory_propane_m3,
        volstartbutane as opening_inventory_butane_m3,
        volendpropane as closing_inventory_propane_m3,
        volendbutane as closing_inventory_butane_m3,
        volchginvpropane as change_in_inventory_propane_m3,
        volchginvbutane as change_in_inventory_butane_m3,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed