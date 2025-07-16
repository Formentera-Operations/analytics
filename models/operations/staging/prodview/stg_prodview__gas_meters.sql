{{ config(
    materialized='view',
    tags=['prodview', 'gas_meters', 'reference', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERORIFICE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as gas_meter_id,
        idrecparent as parent_gas_meter_id,
        idflownet as flow_network_id,
        
        -- Meter information
        name as meter_name,
        entrysource as entry_source,
        typreading as chart_reading_type,
        typrecording as recording_type,
        
        -- Measurement configuration
        uomgasstat as static_pressure_units,
        uomgasdiff as differential_pressure_units,
        uomtemp as temperature_units,
        uomszorifice as orifice_size_units,
        
        -- Physical specifications (converted to US units)
        szrun / 0.0254 as run_size_inches,
        metaltypepipe as pipe_material,
        metaltypeplate as plate_material,
        
        -- Chart and tap configuration
        chartperiod as chart_period,
        typtap as tap_type,
        locstatictap as static_tap_location,
        locprovtap as proving_tap_location,
        
        -- Operational settings
        estmissingday as estimate_missing_days,
        
        -- Reference relationships
        idrecduropsource as source_of_operating_hours_id,
        idrecduropsourcetk as source_of_operating_hours_table,
        idrecunitnodecalc as node_id,
        idrecunitnodecalctk as node_table,
        idrecunitdataentryor as data_entry_unit_id,
        idrecunitdataentryortk as data_entry_unit_table,
        
        -- Meter identifiers
        serialnum as serial_number,
        engineeringid as engineering_id,
        regulatoryid as regulatory_id,
        otherid as other_id,
        
        -- Operational configuration
        entryreqperiod as entry_requirement_period,
        
        -- Import/Export configuration
        importid1 as import_id_1,
        importtyp1 as import_type_1,
        importid2 as import_id_2,
        importtyp2 as import_type_2,
        exportid1 as export_id_1,
        exporttyp1 as export_type_1,
        exportid2 as export_id_2,
        exporttyp2 as export_type_2,
        
        -- Migration and organization
        keymigrationsource as migration_source_key,
        typmigrationsource as migration_source_type,
        sysseq as system_sequence,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        
        -- Administrative fields
        dttmhide as hide_record_as_of,
        
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