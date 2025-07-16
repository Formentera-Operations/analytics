{{ config(
    materialized='view',
    tags=['prodview', 'measurements', 'reference', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITMEASPT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as measurement_point_id,
        idrecparent as parent_measurement_point_id,
        idflownet as flow_network_id,
        
        -- Measurement point information
        name as measurement_name,
        idrecloc as measurement_location_id,
        idrecloctk as measurement_location_table,
        refida as reference_id,
        uomreading as reading_unit_of_measure,
        entryreqperiod as entry_requirement_period,
        com as comment,
        
        -- Import/Export configuration
        importid1 as import_id_1,
        importtyp1 as import_type_1,
        importid2 as import_id_2,
        importtyp2 as import_type_2,
        exportid1 as export_id_1,
        exporttyp1 as export_type_1,
        exportid2 as export_id_2,
        exporttyp2 as export_type_2,
        
        -- Grouping and organization
        groupkey as group_key,
        groupname as group_name,
        sysseq as system_sequence,
        
        -- Migration and carry forward
        keymigrationsource as migration_source_key,
        typmigrationsource as migration_source_type,
        
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