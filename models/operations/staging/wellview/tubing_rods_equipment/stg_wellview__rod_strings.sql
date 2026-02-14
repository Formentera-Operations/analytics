{{ config(
    materialized='view',
    tags=['wellview', 'rod', 'strings', 'artificial_lift', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVROD') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as rod_string_id,
        idwell as well_id,

        -- Basic string information
        proposedoractual as proposed_or_actual,
        propversionno as proposed_version_number,
        des as rod_string_description,
        com as comments,

        -- Job relationships
        idrecjobrun as run_job_id,
        idrecjobruntk as run_job_table_key,
        idrecjobpull as pull_job_id,
        idrecjobpulltk as pull_job_table_key,
        idrecjobprogramphasecalc as program_phase_id,
        idrecjobprogramphasecalctk as program_phase_table_key,

        -- Wellbore and tubing relationships
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idrectub as tubing_string_id,
        idrectubtk as tubing_string_table_key,

        -- Depths and measurements (converted to US units)
        gradecalc as string_grade,
        connthrdtopcalc as top_connection_thread,
        componentscalc as string_components_description,
        dttmrun as run_datetime,
        dttmpickup as pickup_datetime,

        -- String specifications (converted to US units)
        dttmonbottom as on_bottom_datetime,
        dttmoutofhole as out_of_hole_datetime,
        dttmpull as pull_datetime,
        dttmproppull as proposed_pull_datetime,
        pullreason as pull_reason,

        -- String properties
        pullreasondetail as pull_reason_detail,
        durruntopullcalc as duration_run_to_pull_days,

        -- Operational dates
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,

        -- Pull operations
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        complexityindex as complexity_index,

        -- Calculated durations and performance (converted to US units)
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Related entities
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,

        -- User fields
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,

        -- System fields
        depthbtm / 0.3048 as set_depth_ft,
        depthtopcalc / 0.3048 as top_depth_ft,
        depthtvdbtmcalc / 0.3048 as set_depth_tvd_ft,
        lengthcalc / 0.3048 as rod_string_length_ft,
        stickupkbcalc / 0.3048 as stick_up_kb_ft,
        szodnommaxcalc / 0.0254 as string_nominal_od_in,
        szodnomcompmaxcalc / 0.0254 as max_component_nominal_od_in,
        wtperlengthcalc / 1.48816394356955 as weight_per_length_lb_per_ft,
        coalesce(tapered = 1, false) as is_tapered_string,
        duronbottomtopickupcalc / 0.0416666666666667 as duration_on_bottom_to_pickup_hours,

        -- Fivetran fields
        depthonbtmtopickupcalc / 7.3152 as depth_on_bottom_to_pickup_ft_per_hour

    from source_data
)

select * from renamed
