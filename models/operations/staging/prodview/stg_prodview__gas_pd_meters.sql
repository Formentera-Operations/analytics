{{ config(
    materialized='view',
    tags=['prodview', 'meters', 'gas_pd', 'configuration', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERPDGAS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as meter_id,
        idrecparent as parent_unit_id,
        idflownet as flow_network_id,
        
        -- Meter configuration
        name as meter_name,
        uomvol as reading_units,
        typrecording as recording_type,
        locprovtap as proving_tap_location,
        
        -- Base conditions (converted to US units)
        tempbase / 0.555555555555556 + 32 as meter_base_temperature_f,
        presbase / 6.894757 as meter_base_pressure_psi,
        corrprestemp as correct_to_network_pres_and_temp,
        
        -- Meter settings
        rezerostart as zero_start,
        readingrollover as reading_rollover_value,
        estmissingday as estimate_missing_days,
        idealgas as assume_ideal_gas,
        
        -- Identification numbers
        serialnum as serial_number,
        engineeringid as engineering_id,
        regulatoryid as regulatory_id,
        otherid as other_id,
        
        -- Operational settings
        entryreqperiod as entry_requirement_period,
        dttmhide as hide_record_date,
        
        -- Node and data entry references
        idrecunitnodecalc as node_id,
        idrecunitnodecalctk as node_table,
        idrecunitdataentryor as data_entry_unit_id,
        idrecunitdataentryortk as data_entry_unit_table,
        
        -- Import/Export tracking
        importid1 as import_id_1,
        importtyp1 as import_type_1,
        importid2 as import_id_2,
        importtyp2 as import_type_2,
        exportid1 as export_id_1,
        exporttyp1 as export_type_1,
        exportid2 as export_id_2,
        exporttyp2 as export_type_2,
        
        -- Migration tracking
        keymigrationsource as migration_source_key,
        typmigrationsource as migration_source_type,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        
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