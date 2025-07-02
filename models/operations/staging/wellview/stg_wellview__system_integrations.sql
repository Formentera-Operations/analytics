{{ config(
    materialized='view',
    tags=['wellview', 'system', 'integration', 'metadata', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview', 'WVT_WVSYSINTEGRATION') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as system_integration_id,
        idrecparent as parent_record_id,
        idwell as well_id,
        
        -- Parent table information
        tblkeyparent as parent_table_key,
        
        -- Integration system details
        integratordes as integrator_description,
        integratorver as integrator_version,
        afproduct as product_description,
        
        -- External system identifiers
        afidentity as af_entity_id,
        afidrec as af_record_id,
        
        -- Additional information
        note as integration_notes,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at
        
    from source_data
)

select * from renamed