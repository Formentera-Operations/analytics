{{ config(
    materialized='view',
    tags=['prodview', 'units', 'agreements', 'partnerships', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITAGREEMTPARTNER') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as agreement_partner_id,
        idrecparent as agreement_id,
        idflownet as flow_network_id,
        
        -- Partner information
        idrecpartner as partner_id,
        idrecpartnertk as partner_table,
        
        -- Product and interest
        typfluidprod as product_type,
        subtypfluidprod as sub_product_type,
        interest / 0.01 as interest_percentage,
        
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