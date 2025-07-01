{{ config(
    materialized='view',
    tags=['wellview', 'wellhead', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEAD') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as wellhead_id,
        idwell as well_id,
        
        -- Job and string relationships
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        idrecstring as annulus_string_id,
        idrecstringtk as annulus_string_table_key,
        idrecjobprogramphasecalc as program_phase_id,
        idrecjobprogramphasecalctk as program_phase_table_key,
        
        -- Basic wellhead information
        proposedoractual as proposed_or_actual,
        proprunversionno as proposed_run_version_number,
        typ as wellhead_type,
        make as manufacturer,
        profile as wellhead_profile,
        service as service_type,
        class as wellhead_class,
        
        -- Physical specifications (converted to US units)
        sz / 0.0254 as wellhead_size_in,
        depthbtm / 0.3048 as set_depth_ft,
        
        -- Pressure ratings (converted to US units)
        workpres / 6.894757 as working_pressure_psi,
        maxpres / 6.894757 as maximum_pressure_psi,
        
        -- Temperature ratings (converted to US units)
        temprating / 0.555555555555556 + 32 as temperature_rating_f,
        tempratingdes as temperature_rating_description,
        
        -- Specification details
        productspeclevel as product_specification_level,
        
        -- Installation and operational dates
        dttmstart as installation_datetime,
        dttmend as removal_datetime,
        dttmoverhaul as overhaul_datetime,
        dttmpropend as proposed_removal_datetime,
        
        -- Operational information
        removereason as removal_reason,
        com as comments,
        
        -- Calculated relationships
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
        -- Metric equivalents for reference
        sz as wellhead_size_m,
        depthbtm as set_depth_m,
        workpres as working_pressure_kpa,
        maxpres as maximum_pressure_kpa,
        temprating as temperature_rating_c,
        
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