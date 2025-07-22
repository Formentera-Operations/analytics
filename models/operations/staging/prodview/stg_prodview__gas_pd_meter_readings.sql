{{ config(
    materialized='view',
    tags=['prodview', 'meters', 'gas_pd', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERPDGASENTRY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as meter_entry_id,
        idrecparent as parent_meter_id,
        idflownet as flow_network_id,
        
        -- Date/Time and readings
        dttm as reading_date,
        readingend as reading_value,
        
        -- Temperature and pressure (converted to US units)
        temp / 0.555555555555556 + 32 as temperature_f,
        pres / 6.894757 as pressure_psi,
        
        -- Calculated volume (converted to MCF)
        volgascalc / 28.316846592 as calculated_gas_volume_mcf,
        
        -- Override values
        readingendor as reading_override,
        readingendorreason as reading_override_reason,
        readingstartor as start_reading_override,
        reasonor as start_override_reason,
        
        -- Heat content (converted to US units)
        heat / 1055055852.62 as heat_mmbtu,
        factheat / 37258.9458078313 as heat_factor_btu_per_ft3,
        
        -- Regulatory codes
        regulatorycode1 as regulatory_code_1,
        regulatorycode2 as regulatory_code_2,
        regulatorycode3 as regulatory_code_3,
        
        -- Comments
        com as note,
        comffv as ffv_note,
        
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