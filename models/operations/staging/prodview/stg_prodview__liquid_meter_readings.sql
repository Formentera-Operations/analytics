{{ config(
    materialized='view',
    tags=['prodview', 'meters', 'liquid', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUIDENTRY') }}
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
        
        -- Meter readings
        readingstart as reading_start,
        readingend as reading_end,
        
        -- Quality measurements (converted to percentages)
        bsw / 0.01 as basic_sediment_water_pct,
        sandcut / 0.01 as sand_cut_pct,
        
        -- Uncorrected volumes (converted to US units)
        voluncorrtotalcalc / 0.158987294928 as uncorrected_total_volume_bbl,
        voluncorrhcliqcalc / 0.158987294928 as uncorrected_hcliq_volume_bbl,
        
        -- Sample conditions (converted to US units)
        tempofvol / 0.555555555555556 + 32 as volume_temperature_f,
        presofvol / 6.894757 as volume_pressure_psi,
        tempsample / 0.555555555555556 + 32 as sample_temperature_f,
        pressample / 6.894757 as sample_pressure_psi,
        
        -- Density measurements (converted to API gravity)
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as sample_density_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as sample_density_60f_api,
        
        -- Corrected volumes (converted to US units)
        volcorrtotalcalc / 0.158987294928 as corrected_total_volume_bbl,
        volcorrhcliqcalc / 0.158987294928 as corrected_hcliq_volume_bbl,
        
        -- Corrected quality measurements (converted to percentages)
        bswcorrcalc / 0.01 as corrected_bsw_pct,
        sandcutcorrcalc / 0.01 as corrected_sand_cut_pct,
        
        -- Override conditions (converted to US units)
        tempor / 0.555555555555556 + 32 as override_temperature_f,
        presor / 6.894757 as override_pressure_psi,
        power(nullif(densityor, 0), -1) / 7.07409872233005E-06 + -131.5 as override_density_api,
        reasonor as override_reason,
        
        -- Override volumes (converted to US units)
        volorhcliq / 0.158987294928 as override_hcliq_volume_bbl,
        volorwater / 0.158987294928 as override_water_volume_bbl,
        volorsand / 0.158987294928 as override_sand_volume_bbl,
        
        -- Final calculated volumes (converted to US units)
        voltotalcalc / 0.158987294928 as total_volume_bbl,
        volhcliqcalc / 0.158987294928 as hcliq_volume_bbl,
        volhcliqgaseqcalc / 28.316846592 as hcliq_gas_equivalent_mcf,
        volwatercalc / 0.158987294928 as water_volume_bbl,
        volsandcalc / 0.158987294928 as sand_volume_bbl,
        
        -- Final calculated quality (converted to percentages)
        bswcalc / 0.01 as final_bsw_pct,
        sandcutcalc / 0.01 as final_sand_cut_pct,
        
        -- Ticket information
        ticketno as ticket_number,
        ticketsubno as ticket_sub_number,
        
        -- Reference and tracking
        refid as reference_id,
        origstatementid as original_statement_id,
        source as data_source,
        verified as is_verified,
        
        -- Analysis and seal references
        idrechcliqanalysiscalc as hc_liquid_analysis_id,
        idrechcliqanalysiscalctk as hc_liquid_analysis_table,
        idrecsealentry as seal_entry_id,
        idrecsealentrytk as seal_entry_table,
        
        -- Comments
        com as comments,
        
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