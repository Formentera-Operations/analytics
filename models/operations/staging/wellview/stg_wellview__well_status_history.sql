{{ config(
    materialized='view',
    tags=['wellview', 'well-status', 'history', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLSTATUSHISTORY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifier
        idwell as well_id,
        idrec as record_id,

        -- Core status information
        dttm as status_date,
        wellstatus1 as well_status,
        wellstatus2 as well_sub_status,
        welltyp1 as well_type,
        welltyp2 as well_subtype,
        primaryfluiddes as primary_fluid_type,
        source as status_source,
        
        -- Description
        com as comment,

        -- System locking fields
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System tracking fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed