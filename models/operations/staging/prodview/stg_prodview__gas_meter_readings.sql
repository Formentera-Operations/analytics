{{ config(
    materialized='view',
    tags=['prodview', 'meters', 'orifice', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERORIFICEENTRY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as meter_entry_id,
        idrecparent as parent_meter_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttm as reading_date,
        
        -- Duration (converted to hours)
        duronor / 0.0416666666666667 as override_duration_hours,
        duroncalc / 0.0416666666666667 as calculated_duration_hours,
        
        -- Pressure readings - raw values
        presstatic as static_pressure_raw,
        presdiff as differential_pressure_raw,
        temp as temperature_raw,
        
        -- Pressure readings - calculated (converted to PSI)
        presstaticcalc / 6.894757 as calculated_static_pressure_psi,
        presdiffcalc / 6.894757 as calculated_differential_pressure_psi,
        
        -- Temperature - calculated (converted to Fahrenheit)
        tempcalc / 0.555555555555556 + 32 as calculated_temperature_f,
        
        -- Orifice configuration
        cprime as c_prime_factor,
        szorifice as orifice_size,
        
        -- Volume measurements (converted to MCF)
        voluncorrgascalc / 28.316846592 as uncorrected_gas_volume_mcf,
        volentergas / 28.316846592 as entered_gas_volume_mcf,
        volenterorgas / 28.316846592 as override_gas_volume_mcf,
        volgascalc / 28.316846592 as calculated_gas_volume_mcf,
        volsourcecalc as source_volume_calculation,
        
        -- Override reason
        reasonor as override_reason,
        
        -- Heat content - entered values (converted to MMBTU)
        heatenter / 1055055852.62 as entered_heat_mmbtu,
        factheatenter / 37258.9458078313 as entered_heat_factor_btu_per_ft3,
        
        -- Heat content - override values (converted to MMBTU)
        heatenteror / 1055055852.62 as override_heat_mmbtu,
        factheatenteror / 37258.9458078313 as override_heat_factor_btu_per_ft3,
        
        -- Heat content - calculated values (converted to MMBTU)
        heatcalc / 1055055852.62 as calculated_heat_mmbtu,
        factheatcalc / 37258.9458078313 as calculated_heat_factor_btu_per_ft3,
        
        -- Regulatory codes
        regulatorycode1 as regulatory_code_1,
        regulatorycode2 as regulatory_code_2,
        regulatorycode3 as regulatory_code_3,
        
        -- Comments
        com as comments,
        comffv as ffv_comments,
        
        -- Gas analysis reference
        idrecgasanalysiscalc as gas_analysis_id,
        idrecgasanalysiscalctk as gas_analysis_table,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        userdttm1 as user_date_1,
        userdttm2 as user_date_2,
        userdttm3 as user_date_3,
        
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