{{ config(
    materialized='view',
    tags=['prodview', 'gathered', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPGATHMONTHDAYCALC') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as gathered_id,
        idrecparent as parent_gathered_id,
        idflownet as flow_network_id,
        idreccomp as completion_id,
        idreccomptk as completion_table,
        
        -- Date/Time information
        dttm as gathered_date,
        year as gathered_year,
        month as gathered_month,
        dayofmonth as day_of_month,
        
        -- Operational time (converted to hours)
        durop / 0.0416666666666667 as operating_time_hours,
        durdown / 0.0416666666666667 as downtime_hours,
        
        -- Gathered production volumes (converted to US units)
        voltotalliq / 0.158987294928 as total_liquid_bbl,
        volhcliq / 0.158987294928 as gathered_oil_condensate_bbl,
        volgas / 28.316846592 as gathered_gas_mcf,
        volwater / 0.158987294928 as gathered_water_bbl,
        volsand / 0.158987294928 as gathered_sand_bbl,
        volcasinggas / 28.316846592 as casing_gas_mcf,
        
        -- New gathered production (converted to US units)
        volnewprodgathhcliq / 0.158987294928 as new_gathered_oil_condensate_bbl,
        volnewprodgathgas / 28.316846592 as new_gathered_gas_mcf,
        volnewprodgathwater / 0.158987294928 as new_gathered_water_bbl,
        volnewprodgathsand / 0.158987294928 as new_gathered_sand_bbl,
        
        -- Prorated production (converted to US units)
        volproratedhcliq / 0.158987294928 as prorated_hcliq_bbl,
        volproratedgas / 28.316846592 as prorated_gas_mcf,
        volproratedwater / 0.158987294928 as prorated_water_bbl,
        volproratedsand / 0.158987294928 as prorated_sand_bbl,
        
        -- Recoverable load/lift - Starting volumes (converted to US units)
        volgathstartremainrecovhcliq / 0.158987294928 as starting_load_oil_condensate_bbl,
        volgathstartremainrecovgas / 28.316846592 as starting_lift_gas_mcf,
        volgathstartremainrecovwater / 0.158987294928 as starting_load_water_bbl,
        volgathstartremainrecovsand / 0.158987294928 as starting_sand_bbl,
        
        -- Recoverable load/lift - Recovered volumes (converted to US units)
        volgathrecovhcliq / 0.158987294928 as recovered_load_oil_condensate_bbl,
        volgathrecovgas / 28.316846592 as recovered_lift_gas_mcf,
        volgathrecovwater / 0.158987294928 as recovered_load_water_bbl,
        volgathrecovsand / 0.158987294928 as recovered_sand_bbl,
        
        -- Recoverable load/lift - Injected volumes (converted to US units)
        volgathinjectrecovgas / 28.316846592 as injected_lift_gas_mcf,
        volgathinjectrecovhcliq / 0.158987294928 as injected_load_oil_condensate_bbl,
        volgathinjectrecovwater / 0.158987294928 as injected_load_water_bbl,
        volgathinjectrecovsand / 0.158987294928 as injected_sand_bbl,
        
        -- Recoverable load/lift - Remaining volumes (converted to US units)
        volgathremainrecovhcliq / 0.158987294928 as remaining_load_oil_condensate_bbl,
        volgathremainrecovgas / 28.316846592 as remaining_lift_gas_mcf,
        volgathremainrecovwater / 0.158987294928 as remaining_load_water_bbl,
        volgathremainrecovsand / 0.158987294928 as remaining_sand_bbl,
        
        -- Fluid properties
        gor / 178.107606679035 as gas_oil_ratio_mcf_per_bbl,
        
        -- Production rates (converted to US units per day)
        ratetotalliq / 0.1589873 as rate_total_liquid_bbl_per_day,
        ratehcliq / 0.1589873 as rate_oil_condensate_bbl_per_day,
        rategas / 28.316846592 as gas_rate_mcf_per_day,
        ratewater / 0.1589873 as water_rate_bbl_per_day,
        ratesand / 0.1589873 as sand_rate_bbl_per_day,
        
        -- Change in rates (converted to US units per day)
        ratechgtotalliq / 0.1589873 as change_in_total_liquid_rate_bbl_per_day,
        ratechghcliq / 0.1589873 as change_in_oil_condensate_rate_bbl_per_day,
        ratechggas / 28.316846592 as change_in_gas_rate_mcf_per_day,
        ratechgwater / 0.1589873 as change_in_water_rate_bbl_per_day,
        ratechgsand / 0.1589873 as change_in_sand_rate_bbl_per_day,
        
        -- Percent change in rates (converted to percentages)
        pctchgtotliq / 0.01 as pct_change_total_liquid_rate_pct,
        pctchghcliq / 0.01 as pct_change_oil_condensate_rate_pct,
        pctchggas / 0.01 as pct_change_gas_rate_pct,
        pctchgwater / 0.01 as pct_change_water_rate_pct,
        pctchgsand / 0.01 as pct_change_sand_rate_pct,
        
        -- Rate tolerance flags
        rateintol as all_products_rate_within_tolerance,
        ratehcliqintol as oil_condensate_rate_within_tolerance,
        rategasintol as gas_rate_within_tolerance,
        ratewaterintol as water_rate_within_tolerance,
        ratesandintol as sand_rate_within_tolerance,
        
        -- Lost production due to downtime (converted to US units)
        vollosthcliq / 0.158987294928 as deferred_oil_condensate_production_bbl,
        vollostgas / 28.316846592 as deferred_gas_production_mcf,
        vollostwater / 0.158987294928 as deferred_water_production_bbl,
        vollostsand / 0.158987294928 as deferred_sand_production_bbl,
        
        -- Difference from target (converted to US units)
        voldifftargethcliq / 0.158987294928 as difference_from_target_oil_condensate_bbl,
        voldifftargetgas / 28.316846592 as difference_from_target_gas_mcf,
        voldifftargetwater / 0.158987294928 as difference_from_target_water_bbl,
        voldifftargetsand / 0.158987294928 as difference_from_target_sand_bbl,
        
        -- Injection volumes (converted to US units)
        volinjecthcliq / 0.158987294928 as injection_well_oil_cond_bbl,
        volinjectgas / 28.316846592 as injection_well_gas_mcf,
        volinjectwater / 0.158987294928 as injection_well_water_bbl,
        volinjectsand / 0.158987294928 as injection_well_sand_bbl,
        
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