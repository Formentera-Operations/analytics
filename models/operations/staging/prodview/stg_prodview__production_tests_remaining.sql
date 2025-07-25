{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'tests', 'requirements', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTESTREQEXCALC') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as test_requirement_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Period information
        dttmstart as period_start_date,
        dttmend as period_end_date,
        
        -- Test information
        dttmlasttest as last_test_date,
        notestremain as required_tests_remaining,
        
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