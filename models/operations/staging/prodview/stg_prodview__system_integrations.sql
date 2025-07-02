{{ config(
    materialized='view',
    tags=['prodview', 'integration', 'system', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVSYSINTEGRATION') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as integration_id,
        idrecparent as parent_record_id,
        idflownet as flow_network_id,
        tblkeyparent as table_key_parent,
        
        -- Integration system information
        integratordes as integrator_description,
        integratorver as integrator_version,
        afproduct as product_description,
        
        -- External system references
        afidentity as af_id_entity,
        afidrec as af_id_rec,
        
        -- Notes and documentation
        note as note,
        
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