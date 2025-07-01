{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'mandrels', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPMANDREL') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as mandrel_id,
        idrecparent as tubing_component_id,
        idwell as well_id,
        
        -- Mandrel station information
        stationno as station_number,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        
        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed