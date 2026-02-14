{{ config(
    materialized='view',
    tags=['wellview', 'casing', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCAS') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as casing_string_id,
        idwell as well_id,
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,

        -- Job identifiers
        idrecjobrun as run_job_id,
        idrecjobruntk as run_job_table_key,
        idrecjobpull as pull_job_id,
        idrecjobpulltk as pull_job_table_key,
        idrecjobprogramphasecalc as program_phase_id,
        idrecjobprogramphasecalctk as program_phase_table_key,

        -- Basic string information
        des as casing_description,
        proposedoractual as proposed_or_actual,
        propversionno as proposed_version_number,
        com as comments,

        -- Depths and measurements (converted to US units)
        gradecalc as string_grade,
        connthrdtopcalc as top_connection_thread,
        componentscalc as string_components,
        compcasdimcalc as tapered_string_dimensions,
        compcasdimszodnomcalc as tapered_string_od_nominal,
        compcaslengthcalc as component_casing_length,
        centralizers,

        -- String specifications (converted to US units)
        scratchers,
        centralizersnotallycalc as centralizer_count_tally,
        contractor as makeup_contractor,
        latposition as lateral_position,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,

        -- String properties
        dttmrun as run_datetime,
        dttmpickup as pickup_datetime,
        dttmonbottom as on_bottom_datetime,
        dttmoutofhole as out_of_hole_datetime,
        dttmpull as pull_datetime,

        -- Weights and forces (converted to US units)
        dttmproppull as proposed_pull_datetime,
        pullreason as pull_reason,
        pullreasondetail as pull_reason_detail,
        dttmcutpull as cut_pull_datetime,

        -- Volumes and pressures (converted to US units)
        dttmpropcutpull as proposed_cut_pull_datetime,
        depthcutpull as cut_pull_depth_meters,
        depthtvdcutpullcalc as cut_pull_depth_tvd_meters,
        reasoncutpull as cut_pull_reason,
        notecutpull as cut_pull_note,
        durruntopullcalc as duration_run_to_pull_days,

        -- Centralizers and accessories (converted percentages)
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        complexityindex as complexity_index,
        syscreatedate as created_at,

        -- Operational information (converted to US units)
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,

        -- Dates - Run operations
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        depthbtm / 0.3048 as set_depth_ft,

        -- Dates - Pull operations
        depthtopcalc / 0.3048 as top_depth_ft,
        depthtvdbtmcalc / 0.3048 as set_depth_tvd_ft,
        lengthcalc / 0.3048 as string_length_ft,
        stickupkbcalc / 0.3048 as stick_up_kb_ft,

        -- Dates - Cut and pull operations
        depthcutpull / 0.3048 as cut_pull_depth_ft,
        depthtvdcutpullcalc / 0.3048 as cut_pull_depth_tvd_ft,
        szodnommaxcalc / 0.0254 as max_nominal_od_in,
        szodnomcompmaxcalc / 0.0254 as max_component_nominal_od_in,
        szidnommincalc / 0.0254 as min_nominal_id_in,
        szidnomcompmincalc / 0.0254 as min_component_nominal_id_in,

        -- Calculated durations (converted to US units)
        szdriftmincalc / 0.0254 as min_drift_size_in,
        wtperlengthcalc / 1.48816394356955 as weight_per_length_lb_per_ft,
        coalesce(tapered = 1, false) as is_tapered,

        -- User fields
        stringwtup / 4448.2216152605 as string_weight_up_klbf,
        stringwtdown / 4448.2216152605 as string_weight_down_klbf,
        travelequipwt / 4448.2216152605 as travel_equipment_weight_klbf,
        tension / 4448.2216152605 as set_tension_kips,

        -- System fields
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumeshoetrack / 0.158987294928 as shoe_track_volume_bbl,
        operatingpresslimit / 6.894757 as operating_pressure_limit_psi,
        leakoffprescalc / 6.894757 as leak_off_pressure_psi,
        leakoffdensityfluidcalc / 119.826428404623 as leak_off_fluid_density_ppg,
        totalstretchsumcalc / 0.3048 as total_stretch_ft,
        centralizersstandoffavg / 0.01 as centralizer_standoff_avg_percent,
        centralizersstandoffmin / 0.01 as centralizer_standoff_min_percent,
        wellboreszcalc / 0.0254 as wellbore_size_in,
        duronbottomtopickupcalc / 0.0416666666666667 as duration_on_bottom_to_pickup_hours,

        -- Fivetran fields
        depthonbtmtopickupcalc / 7.3152 as depth_on_bottom_to_pickup_ft_per_hour

    from source_data
)

select * from renamed
