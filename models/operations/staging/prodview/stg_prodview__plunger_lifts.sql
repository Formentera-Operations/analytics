{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'plunger_lift', 'equipment', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMPPLUNGER') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as plunger_lift_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Plunger specifications
        displacement / 1.104078437E-06 as displacement_rate_bbl_per_day_per_100rpm,
        
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