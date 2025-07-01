{{ config(
    materialized='view',
    tags=['wellview', 'cement', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCEMENTSTAGEFLUID') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as cement_stage_fluid_id,
        idwell as well_id,
        idrecparent as cement_stage_id,
        
        -- Fluid classification
        typ as fluid_type,
        objective as fluid_objective,
        cmtclass as cement_class,
        watersource as water_source,
        
        -- Fluid description
        desfluid as fluid_description,
        com as comments,
        
        -- Depths and locations (converted to US units)
        depthtopest / 0.3048 as estimated_top_depth_ft,
        depthbtmest / 0.3048 as estimated_bottom_depth_ft,
        depthtvdtopestcalc / 0.3048 as estimated_top_depth_tvd_ft,
        depthtvdbtmestcalc / 0.3048 as estimated_bottom_depth_tvd_ft,
        
        -- Timing information
        dttmmix as mix_datetime,
        dttmstartpump as pump_start_datetime,
        dttmendpump as pump_end_datetime,
        
        -- Cement quantities (converted to US units)
        amtcement / 45.359237 as cement_amount_sacks,
        volpumped / 0.158987294928 as volume_pumped_bbl,
        excesspumped / 0.01 as excess_pumped_percent,
        
        -- Mix design properties (converted to US units)
        mixwaterratio / 8.34540445201933E-05 as mix_water_ratio_gal_per_sack,
        yield / 0.000624279605761446 as cement_yield_ft3_per_sack,
        density / 119.826428404623 as fluid_density_ppg,
        
        -- Rheological properties (converted to US units)
        plasticvis / 0.001 as plastic_viscosity_cp,
        yieldpt / 0.000478802589803 as yield_point_lbf_per_100ft2,
        freewater / 0.01 as free_water_percent,
        filtrate / 4.8E-05 as filtrate_loss_ml_per_30min,
        
        -- Temperature properties (converted to US units)
        tempvisc / 0.555555555555556 + 32 as fan_temperature_f,
        thickentemp / 0.555555555555556 + 32 as thickening_temperature_f,
        comprstrtemp / 0.555555555555556 + 32 as compressive_strength_test_temp_f,
        
        -- Time properties (converted to US units)
        thickentm / 0.0416666666666667 as thickening_time_hours,
        comprstrtm1 / 0.0416666666666667 as first_compressive_strength_test_time_hours,
        comprstrtm2 / 0.0416666666666667 as second_compressive_strength_test_time_hours,
        
        -- Pressure properties (converted to US units)
        presfinal / 6.894757 as final_pressure_psi,
        presfrictionloss / 6.894757 as friction_pressure_loss_psi,
        
        -- Strength properties (converted to US units)
        comprstr1 / 6.894757 as first_compressive_strength_psi,
        comprstr2 / 6.894757 as second_compressive_strength_psi,
        
        -- Rate properties (converted to US units)
        rateavg / 228.941712 as average_pump_rate_bbl_per_min,
        
        -- Metric equivalents for reference
        amtcement as cement_amount_kg,
        volpumped as volume_pumped_m3,
        depthtopest as estimated_top_depth_m,
        depthbtmest as estimated_bottom_depth_m,
        depthtvdtopestcalc as estimated_top_depth_tvd_m,
        depthtvdbtmestcalc as estimated_bottom_depth_tvd_m,
        mixwaterratio as mix_water_ratio_m3_per_kg,
        yield as cement_yield_m3_per_kg,
        density as fluid_density_kg_per_m3,
        plasticvis as plastic_viscosity_pa_s,
        yieldpt as yield_point_pa,
        tempvisc as fan_temperature_c,
        thickentemp as thickening_temperature_c,
        comprstrtemp as compressive_strength_test_temp_c,
        thickentm as thickening_time_days,
        comprstrtm1 as first_compressive_strength_test_time_days,
        comprstrtm2 as second_compressive_strength_test_time_days,
        presfinal as final_pressure_kpa,
        presfrictionloss as friction_pressure_loss_kpa,
        comprstr1 as first_compressive_strength_kpa,
        comprstr2 as second_compressive_strength_kpa,
        rateavg as average_pump_rate_m3_per_day,
        filtrate as filtrate_loss_m3_per_day,
        
        -- System fields
        sysseq as sequence_number,
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