{{ config(
    materialized='view',
    tags=['prodview', 'units', 'agreements', 'partnerships', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITAGREEMT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as agreement_id,
        idrecparent as unit_id,
        idflownet as flow_network_id,
        
        -- Agreement details
        des as agreement_description,
        typ1 as agreement_type,
        subtyp1 as agreement_subtype_1,
        subtyp2 as agreement_subtype_2,
        
        -- Agreement period
        dttmstart as agreement_start_date,
        dttmend as agreement_end_date,
        
        -- Agreement application
        idrecappliesto as applies_to_id,
        idrecappliestotk as applies_to_table,
        
        -- Reference IDs
        refida as wi_partner,
        refidb as reference_id_b,
        refidc as reference_id_c,
        
        -- Comments
        com as comments,
        
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