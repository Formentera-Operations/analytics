{{ config(
    materialized='view',
    tags=['prodview', 'parameters', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPARAM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as parameter_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Measurement date
        dttm as measurement_date,
        
        -- Pressure measurements (converted to PSI)
        prestub / 6.894757 as tubing_pressure_psi,
        prescas / 6.894757 as casing_pressure_psi,
        presannulus / 6.894757 as annulus_pressure_psi,
        presline / 6.894757 as line_pressure_psi,
        presinj / 6.894757 as injection_pressure_psi,
        preswh / 6.894757 as wellhead_pressure_psi,
        presbh / 6.894757 as bottomhole_pressure_psi,
        prestubsi / 6.894757 as shut_in_tubing_pressure_psi,
        prescassi / 6.894757 as shut_in_casing_pressure_psi,
        
        -- Temperature measurements (converted to Fahrenheit)
        tempwh / 0.555555555555556 + 32 as wellhead_temperature_f,
        tempbh / 0.555555555555556 + 32 as bottomhole_temperature_f,
        
        -- Equipment specifications
        szchoke / 0.000396875 as choke_size_64ths,
        
        -- Fluid properties
        viscdynamic as dynamic_viscosity_pascal_seconds,
        visckinematic / 55.741824 as kinematic_viscosity_in2_per_s,
        ph as ph_level,
        salinity / 1E-06 as h2s_daily_reading_ppm,
        
        -- User-defined pressure measurements (converted to PSI)
        presuser1 / 6.894757 as surface_casing_pressure_psi,
        presuser2 / 6.894757 as intermediate_casing_pressure_psi,
        presuser3 / 6.894757 as plunger_on_pressure_psi,
        presuser4 / 6.894757 as user_pressure_4_psi,
        presuser5 / 6.894757 as annulus_pressure_2_psi,
        
        -- User-defined temperature measurements (converted to Fahrenheit)
        tempuser1 / 0.555555555555556 + 32 as treater_temperature_f,
        tempuser2 / 0.555555555555556 + 32 as user_temperature_2_f,
        tempuser3 / 0.555555555555556 + 32 as user_temperature_3_f,
        tempuser4 / 0.555555555555556 + 32 as fluid_level_csg_p_psi_f,
        tempuser5 / 0.555555555555556 + 32 as fluid_level_tbg_p_psi_f,
        
        -- User-defined fields - Text (plunger information)
        usertxt1 as spcc_inspection_complete,
        usertxt2 as plunger_model,
        usertxt3 as plunger_make,
        usertxt4 as plunger_size,
        usertxt5 as operational_work,
        
        -- User-defined fields - Numeric (plunger operations)
        usernum1 as cycles,
        usernum2 as arrivals,
        usernum3 / 0.000694444444444444 as travel_time_min,
        usernum4 / 0.000694444444444444 as after_flow_min,
        usernum5 / 0.000694444444444444 as shut_in_time_min,
        
        -- User-defined fields - Datetime
        userdttm1 as plunger_inspection_date,
        userdttm2 as plunger_replace_date,
        userdttm3 as user_date_3,
        userdttm4 as user_date_4,
        userdttm5 as user_date_5,
        
        -- Notes and comments
        com as note,
        
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