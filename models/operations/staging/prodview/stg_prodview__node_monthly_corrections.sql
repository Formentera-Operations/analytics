{{ config(
    materialized='view',
    tags=['prodview', 'nodes', 'corrections', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITNODECORR') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as node_correction_id,
        idrecparent as parent_node_id,
        idflownet as flow_network_id,
        
        -- Date range
        dttmstart as start_of_correction,
        dttmend as end_of_correction,
        
        -- Reference information
        refid as reference_number,
        origstatementid as statement_id,
        source as data_source,
        
        -- Temperature measurements (converted to Fahrenheit)
        temp / 0.555555555555556 + 32 as temperature_f,
        tempsample / 0.555555555555556 + 32 as temperature_of_sample_f,
        
        -- Pressure measurements (converted to PSI)
        pres / 6.894757 as pressure_psi,
        pressample / 6.894757 as pressure_of_sample_psi,
        
        -- Density measurements (converted to API gravity)
        power(nullif(density, 0), -1) / 7.07409872233005E-06 + -131.5 as density_api,
        power(nullif(density60f, 0), -1) / 7.07409872233005E-06 + -131.5 as density_at_60f_api,
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as density_of_sample_api,
        
        -- Final corrected volumes (converted to US units)
        volhcliq / 0.158987294928 as final_corrected_hcliq_bbl,
        volgas / 28.316846592 as final_gas_mcf,
        volwater / 0.158987294928 as final_water_bbl,
        volsand / 0.158987294928 as final_sand_bbl,
        
        -- Heat values (converted to US units)
        heat / 1055055852.62 as final_corrected_heat_mmbtu,
        factheat / 37258.9458078313 as final_corrected_heat_factor_btu_per_ft3,
        
        -- Heat estimates and differences
        estheat / 1055055852.62 as estimated_heat_mmbtu,
        diffheat / 1055055852.62 as heat_difference_mmbtu,
        pctdiffheat / 0.01 as heat_correction_pct,
        
        -- Gas estimates and differences (converted to MCF)
        volestgas / 28.316846592 as estimated_gas_volume_mcf,
        voldiffgas / 28.316846592 as volume_difference_gas_mcf,
        pctdiffgas / 0.01 as gas_correction_pct,
        
        -- HCLiq estimates and differences (converted to barrels)
        volesthcliq / 0.158987294928 as estimated_volume_hcliq_bbl,
        voldiffhcliq / 0.158987294928 as volume_difference_hcliq_bbl,
        pctdiffhcliq / 0.01 as hcliq_correction_pct,
        
        -- Water estimates and differences (converted to barrels)
        volestwater / 0.158987294928 as estimated_volume_water_bbl,
        voldiffwater / 0.158987294928 as volume_difference_water_bbl,
        pctdiffwater / 0.01 as water_correction_pct,
        
        -- Sand estimates and differences (converted to barrels)
        volestsand / 0.158987294928 as estimated_volume_sand_bbl,
        voldiffsand / 0.158987294928 as volume_difference_sand_bbl,
        pctdiffsand / 0.01 as sand_correction_pct,
        
        -- Duration
        duractual as actual_meter_time_days,
        
        -- Comments
        com as comment,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        usernum1 as pop_btu,
        usernum2 as pop_gallons,
        usernum3 as pop_residue_gas,
        usernum4 as user_number_4,
        usernum5 / 0.555555555555556 + 32 as sample_temperature_f,
        
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