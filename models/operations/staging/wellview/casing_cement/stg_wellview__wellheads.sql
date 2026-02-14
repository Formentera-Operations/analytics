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
        tempratingdes as temperature_rating_description,
        productspeclevel as product_specification_level,

        -- Pressure ratings (converted to US units)
        dttmstart as installation_datetime,
        dttmend as removal_datetime,

        -- Temperature ratings (converted to US units)
        dttmoverhaul as overhaul_datetime,
        dttmpropend as proposed_removal_datetime,

        -- Specification details
        removereason as removal_reason,

        -- Installation and operational dates
        com as comments,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,

        -- Operational information
        idreclastfailurecalctk as last_failure_table_key,
        sz as wellhead_size_m,

        -- Calculated relationships
        depthbtm as set_depth_m,
        workpres as working_pressure_kpa,
        maxpres as maximum_pressure_kpa,
        temprating as temperature_rating_c,

        -- Metric equivalents for reference
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- System fields
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        sz / 0.0254 as wellhead_size_in,
        depthbtm / 0.3048 as set_depth_ft,
        workpres / 6.894757 as working_pressure_psi,
        maxpres / 6.894757 as maximum_pressure_psi,

        -- Fivetran fields
        temprating / 0.555555555555556 + 32 as temperature_rating_f

    from source_data
)

select * from renamed
