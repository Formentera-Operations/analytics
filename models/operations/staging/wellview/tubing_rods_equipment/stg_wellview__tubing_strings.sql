{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'completion', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUB') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tubing_string_id,
        idwell as well_id,
        idrecjobrun as run_job_id,
        idrecjobruntk as run_job_table_key,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,

        -- Basic tubing information
        des as description,
        proposedoractual as proposed_or_actual,
        propversionno as proposed_version_number,

        -- Depths (converted to US units - feet)
        dttmrun as run_date,
        dttmpickup as pickup_date,
        dttmonbottom as on_bottom_date,
        dttmoutofhole as out_of_hole_date,
        dttmpull as pull_date,
        dttmcutpull as cut_pull_date,
        dttmpropcutpull as proposed_cut_pull_date,

        -- Dates
        dttmproppull as proposed_pull_date,
        tapered as is_tapered,
        gradecalc as steel_grade,
        connthrdtopcalc as top_connection_thread,
        componentscalc as components_description,
        comptubdimcalc as tapered_dimensions,
        comptubdimszodnomcalc as tapered_od_description,
        comptublengthcalc as component_length_calc,

        -- Tubing specifications (converted to US units)
        centralizersnotallycalc as centralizer_count,
        contractor as makeup_contractor,
        latposition as lateral_position,
        idrecjobpull as pull_job_id,
        idrecjobpulltk as pull_job_table_key,
        pullreason as pull_reason,

        -- String properties
        pullreasondetail as pull_reason_detail,
        reasoncutpull as cut_pull_reason,
        notecutpull as cut_pull_note,
        durruntopullcalc as run_to_pull_duration_days,
        idrecstring as string_id,
        idrecstringtk as string_table_key,
        idreclastrigcalc as last_rig_id,

        -- Tension and pressure (converted to US units)
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,

        -- String weights (converted to US units - klbf)
        idreclastfailurecalctk as last_failure_table_key,
        idrecjobprogramphasecalc as job_program_phase_id,
        idrecjobprogramphasecalctk as job_program_phase_table_key,

        -- Volumes (converted to US units)
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,

        -- Centralizers (converted from proportions to percentages)
        usertxt3 as user_text_3,
        complexityindex as complexity_index,
        com as comments,

        -- Operational information
        syscreatedate as created_at,
        syscreateuser as created_by,

        -- Pull information
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- Calculated durations and rates
        _fivetran_synced as fivetran_synced_at,
        depthbtm / 0.3048 as set_depth_ft,
        depthtopcalc / 0.3048 as top_depth_ft,

        -- String relationships
        depthtvdbtmcalc / 0.3048 as set_depth_tvd_ft,
        lengthcalc / 0.3048 as total_length_ft,

        -- Last rig and failure references
        stickupkbcalc / 0.3048 as stick_up_kb_ft,
        depthtoplinkcalc / 0.3048 as top_link_depth_ft,
        depthbtmlinkcalc / 0.3048 as bottom_link_depth_ft,
        szodnommaxcalc / 0.0254 as max_od_inches,

        -- Phase information
        szodnomcompmaxcalc / 0.0254 as max_component_od_inches,
        szidnommincalc / 0.0254 as min_id_inches,

        -- Total stretch (converted to feet)
        szidnomcompmincalc / 0.0254 as min_component_id_inches,

        -- User fields
        szdriftmincalc / 0.0254 as min_drift_inches,
        wtperlengthcalc / 1.48816394356955 as weight_per_foot_lbs,
        tension / 4.4482216152605 as set_tension_lbf,

        -- Complexity
        operatingpresslimit / 6.894757 as operating_pressure_limit_psi,

        -- Comments
        stringwtup / 4448.2216152605 as string_weight_up_klbf,

        -- System fields
        stringwtdown / 4448.2216152605 as string_weight_down_klbf,
        stringwtrotating / 4448.2216152605 as string_weight_rotating_klbf,
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumeshoetrack / 0.158987294928 as shoe_track_volume_bbl,
        centralizersstandoffavg / 0.01 as avg_standoff_percent,
        centralizersstandoffmin / 0.01 as min_standoff_percent,
        depthcutpull / 0.3048 as cut_pull_depth_ft,
        depthtvdcutpullcalc / 0.3048 as cut_pull_depth_tvd_ft,
        duronbottomtopickupcalc / 0.0416666666666667 as on_bottom_to_pickup_duration_hours,
        depthonbtmtopickupcalc / 7.3152 as depth_on_bottom_to_pickup_ft_per_hour,

        -- Fivetran metadata
        totalstretchsumcalc / 0.3048 as total_stretch_ft

    from source_data
)

select * from renamed
