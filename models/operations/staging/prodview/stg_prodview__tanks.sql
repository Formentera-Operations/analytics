{{ config(
    materialized='view',
    tags=['prodview', 'tanks', 'reference', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITTANK') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tank_id,
        idrecparent as unit_id,
        idflownet as flow_network_id,
        
        -- Tank information
        name as tank_name,
        dttmstart as start_using_tank,
        dttmend as stop_using_tank,
        
        -- Tank capacity and status (converted to US units)
        volcapacity / 0.158987294928 as tank_capacity_bbl,
        volremaincalc / 0.158987294928 as remaining_capacity_bbl,
        pctfullcalc / 0.01 as capacity_percent_full,
        
        -- Product information
        productname as product,
        initialbsw / 0.01 as initial_bsw_pct,
        facproductname as facility_product,
        
        -- Measurement configuration
        methodmeasure as measurement_method,
        excludefmprod as exclude_from_production,
        uomlevel as level_reading_unit,
        vrmethod as vapor_recovery_method,
        typrecording as recording_type,
        underground as underground_tank,
        
        -- Tank identifiers
        serialnum as serial_number,
        engineeringid as engineering_id,
        regulatoryid as regulatory_id,
        otherid as other_id,
        
        -- Operational configuration
        entryreqperiod as entry_requirement_period,
        idrecunitdataentryor as data_entry_unit_id,
        idrecunitdataentryortk as data_entry_unit_table,
        
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