{{ config(
    materialized='view',
    tags=['prodview', 'allocations', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Record ID",
        idrecparent as "Parent Record ID",
        idflownet as "Flow Net ID",
        idrecunit as "Unit ID",
        idrecunittk as "Unit Table",
        idreccomp as "Completion ID",
        idreccomptk as "Completion Table",
        idreccompzone as "Reporting/Contact Interval ID",
        idreccompzonetk as "Reporting/Contact Interval Table",
        
        -- Date/Time information
        dttm as "Allocation Date",
        year as "Allocation Year",
        month as "Allocation Month",
        dayofmonth as "Allocation Day of Month",
        
        -- Operational time (converted to hours)
        durdown / 0.0416666666666667 as "Downtime Hours",
        durop / 0.0416666666666667 as "Operating Time Hours",
        
        -- Gathered volumes (converted to US units)
        volprodgathhcliq / 0.158987294928 as "Gathered HCLiq bbl",
        volprodgathgas / 28.316846592 as "Gathered Gas mcf",
        volprodgathwater / 0.158987294928 as "Gathered Water bbl",
        volprodgathsand / 0.158987294928 as "Gathered Sand bbl",
        
        -- Allocated volumes (converted to US units)
        volprodallochcliq / 0.158987294928 as "Allocated HCLiq bbl",
        volprodallocoil / 0.158987294928 as "Allocated Oil bbl",
        volprodalloccond / 0.158987294928 as "Allocated Condensate bbl",
        volprodallocngl / 0.158987294928 as "Allocated NGL bbl",
        volprodallochcliqgaseq / 28.316846592 as "Allocated Gas Equivalent of HCLiq mcf",
        volprodallocgas / 28.316846592 as "Allocated Gas mcf",
        volprodallocwater / 0.158987294928 as "Allocated Water bbl",
        volprodallocsand / 0.158987294928 as "Allocated Sand bbl",
        
        -- Allocation factors (unitless ratios)
        AllocFactHCLiq AS "Allocation Factor HCLiq",
        AllocFactGas AS "Allocation Factor Gas",
        AllocFactWater AS "Allocation Factor Water",
        AllocFactSand AS "Allocation Factor Sand",
        
        -- New production volumes (converted to US units)
        volnewprodallochcliq / 0.158987294928 as "New Production HCLiq bbl",
        volnewprodallocoil / 0.158987294928 as "new production oil bbl",
        volnewprodalloccond / 0.158987294928 as "new production condensate bbl",
        volnewprodallocngl / 0.158987294928 as "new production ngl bbl",
        volnewprodallochcliqgaseq / 28.316846592 as "new production hcliq gas equivalent mcf",
        volnewprodallocgas / 28.316846592 as "new production gas mcf",
        volnewprodallocwater / 0.158987294928 as "new production water bbl",
        volnewprodallocsand / 0.158987294928 as "new production_sand_bbl",
        
        -- Working interest (converted to percentages)
        wihcliq / 0.01 as working_interest_oil_cond_pct,
        wigas / 0.01 as working_interest_gas_pct,
        wiwater / 0.01 as working_interest_water_pct,
        wisand / 0.01 as working_interest_sand_pct,
        
        -- Net revenue interest (converted to percentages)
        nrihcliq / 0.01 as net_revenue_interest_oil_cond_pct,
        nrigas / 0.01 as net_revenue_interest_gas_pct,
        nriwater / 0.01 as net_revenue_interest_water_pct,
        nrisand / 0.01 as net_revenue_interest_sand_pct,
        
        -- Lost production due to downtime (converted to US units)
        vollosthcliq / 0.158987294928 as deferred_oil_condensate_production_bbl,
        vollostgas / 28.316846592 as deferred_gas_production_mcf,
        vollostwater / 0.158987294928 as deferred_water_production_bbl,
        vollostsand / 0.158987294928 as deferred_sand_production_bbl,
        
        -- Difference from target (converted to US units)
        voldifftargethcliq / 0.158987294928 as difference_from_target_hcliq_bbl,
        voldifftargetoil / 0.158987294928 as difference_from_target_oil_bbl,
        voldifftargetcond / 0.158987294928 as difference_from_target_condensate_bbl,
        voldifftargetngl / 0.158987294928 as difference_from_target_ngl_bbl,
        voldifftargetgas / 28.316846592 as difference_from_target_gas_mcf,
        voldifftargetwater / 0.158987294928 as difference_from_target_water_bbl,
        voldifftargetsand / 0.158987294928 as difference_from_target_sand_bbl,
        
        -- Recoverable load/lift - Starting volumes (converted to US units)
        volstartremainrecovhcliq / 0.158987294928 as starting_load_oil_condensate_bbl,
        volstartremainrecovgas / 28.316846592 as starting_lift_gas_mcf,
        volstartremainrecovwater / 0.158987294928 as starting_load_water_bbl,
        volstartremainrecovsand / 0.158987294928 as starting_sand_bbl,
        
        -- Recoverable load/lift - Recovered volumes (converted to US units)
        volrecovhcliq / 0.158987294928 as recovered_load_oil_condensate_bbl,
        volrecovgas / 28.316846592 as recovered_lift_gas_mcf,
        volrecovwater / 0.158987294928 as recovered_load_water_bbl,
        volrecovsand / 0.158987294928 as recovered_sand_bbl,
        
        -- Recoverable load/lift - Injected volumes (converted to US units)
        volinjectrecovgas / 28.316846592 as injected_lift_gas_bbl,
        volinjectrecovhcliq / 0.158987294928 as injected_load_oil_condensate_bbl,
        volinjectrecovwater / 0.158987294928 as injected_load_water_bbl,
        volinjectrecovsand / 0.158987294928 as injected_sand_bbl,
        
        -- Recoverable load/lift - Remaining volumes (converted to US units)
        volremainrecovhcliq / 0.158987294928 as remaining_load_oil_condensate_bbl,
        volremainrecovgas / 28.316846592 as remaining_lift_gas_mcf,
        volremainrecovwater / 0.158987294928 as remaining_load_water_bbl,
        volremainrecovsand / 0.158987294928 as remaining_sand_bbl,
        
        -- Opening inventory (converted to US units)
        volstartinvhcliq / 0.158987294928 as opening_inventory_oil_condensate_bbl,
        volstartinvhcliqgaseq / 28.316846592 as opening_inventory_gas_equivalent_oil_cond_mcf,
        volstartinvwater / 0.158987294928 as opening_inventory_water_bbl,
        volstartinvsand / 0.158987294928 as opening_inventory_sand_bbl,
        
        -- Closing inventory (converted to US units)
        volendinvhcliq / 0.158987294928 as closing_inventory_oil_condensate_bbl,
        volendinvhcliqgaseq / 28.316846592 as closing_inventory_gas_equiv_oil_condensate_mcf,
        volendinvwater / 0.158987294928 as closing_inventory_water_bbl,
        volendinvsand / 0.158987294928 as closing_inventory_sand_bbl,
        
        -- Change in inventory (converted to US units)
        volchginvhcliq / 0.158987294928 as change_in_inventory_oil_condensate_bbl,
        volchginvhcliqgaseq / 28.316846592 as change_in_inventory_gas_equivalent_oil_cond_mcf,
        volchginvwater / 0.158987294928 as change_in_inventory_water_bbl,
        volchginvsand / 0.158987294928 as change_in_inventory_sand_bbl,
        
        -- Dispositions - Sales (converted to US units)
        voldispsalehcliq / 0.158987294928 as disposed_allocated_sales_hcliq_bbl,
        voldispsaleoil / 0.158987294928 as disposed_allocated_sales_oil_bbl,
        voldispsalecond / 0.158987294928 as disposed_allocated_sales_condensate_bbl,
        voldispsalengl / 0.158987294928 as disposed_allocated_sales_ngl_bbl,
        voldispsalegas / 28.316846592 as disposed_allocated_sales_gas_mcf,
        
        -- Dispositions - Gas uses (converted to US units)
        voldispfuelgas / 28.316846592 as disposed_allocated_fuel_gas_mcf,
        voldispflaregas / 28.316846592 as disposed_allocated_flare_gas_mcf,
        voldispincinerategas / 28.316846592 as disposed_allocated_incineration_gas_mcf,
        voldispventgas / 28.316846592 as disposed_allocated_vent_gas_mcf,
        voldispinjectgas / 28.316846592 as disposed_allocated_injected_gas_mcf,
        voldispinjectwater / 0.158987294928 as disposed_allocated_injected_water_bbl,
        
        -- Injection well volumes (converted to US units)
        volinjecthcliq / 0.158987294928 as injection_well_oil_cond_bbl,
        volinjectgas / 28.316846592 as injection_well_gas_mcf,
        volinjectwater / 0.158987294928 as injection_well_water_bbl,
        volinjectsand / 0.158987294928 as injection_well_sand_bbl,
        
        -- Cumulative production (converted to US units)
        volprodcumhcliq / 0.158987294928 as cumulated_hcliq_bbl,
        volprodcumoil / 0.158987294928 as cumulated_oil_bbl,
        volprodcumcond / 0.158987294928 as cumulated_condensate_bbl,
        volprodcumngl / 0.158987294928 as cumulated_ngl_bbl,
        volprodcumgas / 28.316846592 as cumulated_gas_mcf,
        volprodcumwater / 0.158987294928 as cumulated_water_bbl,
        volprodcumsand / 0.158987294928 as cumulated_sand_bbl,
        
        -- Heat content (converted to US units)
        heatprodgath / 1055055852.62 as gathered_heat_mmbtu,
        factheatgath / 37258.9458078313 as gathered_heat_factor_btu_per_ft3,
        heatprodalloc / 1055055852.62 as allocated_heat_mmbtu,
        factheatalloc / 37258.9458078313 as allocated_heat_factor_btu_per_ft3,
        heatnewprodalloc / 1055055852.62 as new_production_heat_mmbtu,
        heatdispsale / 1055055852.62 as disposed_sales_heat_mmbtu,
        heatdispfuel / 1055055852.62 as disposed_fuel_heat_mmbtu,
        heatdispflare / 1055055852.62 as disposed_flare_heat_mmbtu,
        heatdispvent / 1055055852.62 as disposed_vent_heat_mmbtu,
        heatdispincinerate / 1055055852.62 as disposed_incinerate_heat_mmbtu,
        
        -- Density (converted to API gravity)
        power(nullif(densityalloc, 0), -1) / 7.07409872233005E-06 + -131.5 as allocated_density_api,
        power(nullif(densitysale, 0), -1) / 7.07409872233005E-06 + -131.5 as sales_density_api,
        
        -- Reference IDs for related records
        idrecmeasmeth as last_measurement_method_id,
        idrecmeasmethtk as last_measurement_method_table,
        idrecfluidlevel as last_fluid_level_id,
        idrecfluidleveltk as last_fluid_level_table,
        idrectest as last_test_id,
        idrectesttk as last_test_table,
        idrecparam as last_param_id,
        idrecparamtk as last_param_table,
        idrecdowntime as downtime_id,
        idrecdowntimetk as downtime_table,
        idrecdeferment as deferment_id,
        idrecdefermenttk as deferment_table,
        idrecgasanalysis as gas_analysis_id,
        idrecgasanalysistk as gas_analysis_table,
        idrechcliqanalysis as hc_liquid_analysis_id,
        idrechcliqanalysistk as hc_liquid_analysis_table,
        idrecoilanalysis as oil_properties_id,
        idrecoilanalysistk as oil_properties_table,
        idrecwateranalysis as water_properties_id,
        idrecwateranalysistk as water_properties_table,
        idrecstatus as status_id,
        idrecstatustk as status_table,
        idrecpumpentry as last_pump_entry_id,
        idrecpumpentrytk as last_pump_entry_table,
        idrecfacility as reporting_facility_id,
        idrecfacilitytk as reporting_facility_table,
        idreccalcset as calc_settings_id,
        idreccalcsettk as calc_settings_table,
        
        -- Other operational metrics
        pumpeff / 0.01 as pump_efficiency_pct,
        
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