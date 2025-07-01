{{ config(
    materialized='view',
    tags=['wellview', 'rig', 'pump', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIGPUMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as rig_pump_id,
        idwell as well_id,
        idrecparent as job_rig_id,
        
        -- Pump identification
        des as pump_number,
        refid as reference_id,
        
        -- Manufacturer information
        make as pump_manufacturer,
        model as pump_model,
        sn as serial_number,
        
        -- Pump classification
        actioncategory as action_category,
        actiontyp as action_type,
        
        -- Physical specifications (converted to US units)
        strokelength / 0.0254 as stroke_length_in,
        szodrod / 0.0254 as rod_diameter_in,
        
        -- Power rating (converted to US units)
        powerrating / 745.6999 as power_rating_hp,
        
        -- Date information
        dttmstart as pump_start_datetime,
        dttmend as pump_end_datetime,
        dttmmanufacture as manufacture_datetime,
        
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