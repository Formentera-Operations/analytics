{{ config(
    materialized='view',
    tags=['prodview', 'tanks', 'entries', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKENTRY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tank_entry_id,
        idrecparent as tank_id,
        idflownet as flow_network_id,
        
        -- Entry information
        dttm as reading_date,
        tankleveltop as level_top,
        bsw / 0.01 as bsw_pct,
        sandcut / 0.01 as sand_cut_pct,
        tanklevelfreewater as water_bottom,
        tanklevelsand as level_sand,
        com as comments,
        
        -- Tank calculations - uncorrected volumes (converted to US units)
        voluncorrtotalcalc / 0.158987294928 as total_volume_uncorrected_bbl,
        voluncorrhcliqcalc / 0.158987294928 as oil_condensate_volume_uncorrected_bbl,
        volfreewatercalc / 0.158987294928 as free_water_volume_bbl,
        volsettledsandcalc / 0.158987294928 as settled_sand_volume_bbl,
        bswuncorrcalc / 0.01 as bsw_uncorrected_pct,
        sandcutuncorrcalc / 0.01 as sand_cut_uncorrected_pct,
        
        -- Temperature and pressure correction data (converted to US units)
        tempofvol / 0.555555555555556 + 32 as temperature_of_volume_f,
        presofvol / 6.894757 as pressure_of_volume_psi,
        tempsample / 0.555555555555556 + 32 as temperature_of_sample_f,
        pressample / 6.894757 as pressure_of_sample_psi,
        
        -- Density measurements (converted to API gravity)
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_60f_api,
        
        -- Change from previous reading (converted to US units)
        volchgtotalcalc / 0.158987294928 as change_in_total_volume_bbl,
        volchghcliqcalc / 0.158987294928 as change_in_oil_condensate_volume_bbl,
        volchghcliqgaseqcalc / 28.316846592 as change_in_gas_equivalent_oil_cond_volume_mcf,
        volchgwatercalc / 0.158987294928 as change_in_water_volume_bbl,
        volchgsandcalc / 0.158987294928 as change_in_sand_volume_bbl,
        
        -- Final volumes (converted to US units)
        voltotalcalc / 0.158987294928 as final_total_fluid_volume_bbl,
        volhcliqcalc / 0.158987294928 as final_hydrocarbon_liquid_volume_bbl,
        volhcliqgaseqcalc / 28.316846592 as final_gas_equivalent_oil_condensate_volume_mcf,
        volwatercalc / 0.158987294928 as final_water_volume_bbl,
        volsandcalc / 0.158987294928 as final_sand_volume_bbl,
        
        -- Final quality measurements (converted to percentages)
        bswcalc / 0.01 as final_bsw_pct,
        sandcutcalc / 0.01 as final_sand_cut_pct,
        
        -- Capacity information (converted to US units)
        volcapacityremaincalc / 0.158987294928 as capacity_remaining_bbl,
        
        -- Analysis references
        idrechcliqanalysiscalc as hc_liquid_analysis_id,
        idrechcliqanalysiscalctk as hc_liquid_analysis_table,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        userdttm1 as user_date_1,
        userdttm2 as user_date_2,
        userdttm3 as load_transfer_request_date,
        
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