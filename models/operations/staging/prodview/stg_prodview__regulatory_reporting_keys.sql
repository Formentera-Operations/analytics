{{ config(
    materialized='view',
    tags=['prodview', 'regulatory', 'reporting_keys', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITREGBODYKEY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as regulatory_key_id,
        idrecparent as parent_unit_id,
        idflownet as flow_network_id,
        
        -- Key information
        keyname as key_name,
        
        -- Item references
        idrecitem as applies_to_item_id,
        idrecitemtk as applies_to_item_table,
        
        -- Date range
        dttmstart as start_date,
        dttmend as end_date,
        
        -- Type classifications
        typ1 as key_type,
        typ2 as key_sub_type,
        
        -- Key values
        keyvalue1 as key_value_1,
        keyvalue2 as key_value_2,
        keyvalue3 as key_value_3,
        
        -- Key numbers
        keynum1 as key_number_1,
        keynum2 as key_number_2,
        keynum3 as key_number_3,
        
        -- Comments
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