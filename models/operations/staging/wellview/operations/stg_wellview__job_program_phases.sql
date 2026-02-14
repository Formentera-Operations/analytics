{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBPROGRAMPHASE') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as parent_record_id,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,

        -- phase classification
        trim(code1)::varchar as phase_type_1,
        trim(code2)::varchar as phase_type_2,
        trim(code3)::varchar as phase_type_3,
        trim(code4)::varchar as phase_type_4,
        trim(code1234calc)::varchar as combined_phase_types,
        trim(des)::varchar as description,

        -- plan information
        durationml::float as planned_likely_duration_days,
        durationmin::float as planned_min_duration_days,
        durationmax::float as planned_max_duration_days,
        durationtechlimit::float as tech_limit_duration_days,
        costml::float as planned_likely_phase_cost,
        costmin::float as planned_min_phase_cost,
        costmax::float as planned_max_phase_cost,
        costtechlimit::float as tech_limit_cost,

        -- planned depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthstartplan') }} as planned_start_depth_ft,
        {{ wv_meters_to_feet('depthendplan') }} as planned_end_depth_ft,
        {{ wv_meters_to_feet('depthtvdstartplancalc') }} as planned_start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdendplancalc') }} as planned_end_depth_tvd_ft,

        -- actual dates
        dttmstartactual::timestamp_ntz as actual_start_date,
        dttmendactual::timestamp_ntz as actual_end_date,
        dttmendcalc::timestamp_ntz as derived_end_date,

        -- actual depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthstartactualcalc') }} as actual_start_depth_ft,
        {{ wv_meters_to_feet('depthendactualcalc') }} as actual_end_depth_ft,
        {{ wv_meters_to_feet('depthtvdstartactualcalc') }} as actual_start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdendactualcalc') }} as actual_end_depth_tvd_ft,
        {{ wv_meters_to_feet('depthprogressactualcalc') }} as actual_depth_progress_ft,
        {{ wv_meters_to_feet('depthprogressplancalc') }} as planned_depth_progress_ft,

        -- length calculations (converted from meters to feet)
        {{ wv_meters_to_feet('lengthactualcalc') }} as actual_phase_length_ft,
        {{ wv_meters_to_feet('lengthplancalc') }} as planned_phase_length_ft,

        -- duration calculations
        durationactualcalc::float as actual_duration_days,
        durationvariancecalc::float as duration_variance_days,
        durationvariancecumcalc::float as cumulative_duration_variance_days,

        -- duration calculations (converted from days to hours)
        {{ wv_days_to_hours('durationiltcalc') }} as invisible_lost_time_hours,
        {{ wv_days_to_hours('durationiltcumcalc') }} as cumulative_invisible_lost_time_hours,
        {{ wv_days_to_hours('durationnoprobtimecalc') }} as time_log_minus_problem_hours,
        {{ wv_days_to_hours('durationnoprobtimecumcalc') }} as cumulative_time_log_minus_problem_hours,
        {{ wv_days_to_hours('durationpersonnelotcalc') }} as personnel_ot_hours,
        {{ wv_days_to_hours('durationpersonnelregcalc') }} as personnel_regular_hours,
        {{ wv_days_to_hours('durationpersonneltotcalc') }} as personnel_total_hours,
        {{ wv_days_to_hours('durationproblemtimecalc') }} as problem_time_hours,
        {{ wv_days_to_hours('durationproblemtimecumcalc') }} as cumulative_problem_time_hours,
        {{ wv_days_to_hours('durationtimelogtotalcalc') }} as time_log_total_hours,

        -- drilling performance times (converted from days to hours)
        {{ wv_days_to_hours('tmdrillcalc') }} as drilling_time_hours,
        {{ wv_days_to_hours('tmdrillcumcalc') }} as cumulative_drilling_time_hours,
        {{ wv_days_to_hours('tmdrillnoexccalc') }} as drilling_time_no_exclusions_hours,
        {{ wv_days_to_hours('tmdrillcumnoexccalc') }} as cumulative_drilling_time_no_exclusions_hours,
        {{ wv_days_to_hours('tmcirccalc') }} as circulating_time_hours,
        {{ wv_days_to_hours('tmcirccumcalc') }} as cumulative_circulating_time_hours,
        {{ wv_days_to_hours('tmothercalc') }} as other_time_hours,
        {{ wv_days_to_hours('tmothercumcalc') }} as cumulative_other_time_hours,
        {{ wv_days_to_hours('tmtripcalc') }} as tripping_time_hours,
        {{ wv_days_to_hours('tmtripcumcalc') }} as cumulative_tripping_time_hours,
        {{ wv_days_to_hours('tmrotatingcalc') }} as rotating_time_hours,
        {{ wv_days_to_hours('tmslidingcalc') }} as sliding_time_hours,
        {{ wv_days_to_hours('tmcirctripothercalc') }} as circ_trip_other_time_hours,
        {{ wv_days_to_hours('tmcirctripothercumcalc') }} as cumulative_circ_trip_other_time_hours,

        -- sensor durations (converted from days to minutes)
        {{ wv_days_to_minutes('duronbtmcalc') }} as duration_on_bottom_minutes,
        {{ wv_days_to_minutes('duroffbtmcalc') }} as duration_off_bottom_minutes,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as duration_pipe_moving_minutes,

        -- rate of penetration (converted from m/s to ft/hr)
        {{ wv_mps_to_ft_per_hr('ropcalc') }} as rop_ft_per_hour,
        {{ wv_mps_to_ft_per_hr('roprotatingcalc') }} as rop_rotating_ft_per_hour,
        {{ wv_mps_to_ft_per_hr('ropslidingcalc') }} as rop_sliding_ft_per_hour,
        power(nullif(ropinstavgcalc, 0), -1) / 0.00227836103820356 as rop_instantaneous_avg_min_per_ft,

        -- cost calculations
        costactualcalc::float as actual_phase_field_est,
        costactualcumcalc::float as actual_phase_cumulative_field_est,
        costactualcumnormcalc::float as actual_phase_cumulative_field_est_normalized,
        costactualnormcalc::float as actual_phase_field_est_normalized,
        costmaxcumcalc::float as max_cumulative_phase_cost,
        costmaxnormcalc::float as max_phase_cost_normalized,
        costmaxnormcumcalc::float as max_cumulative_phase_cost_normalized,
        costmincumcalc::float as min_cumulative_phase_cost,
        costminnormcalc::float as min_phase_cost_normalized,
        costminnormcumcalc::float as min_cumulative_phase_cost_normalized,
        costmlcumcalc::float as likely_cumulative_phase_cost,
        costmlcumnoexcludecalc::float as likely_cumulative_phase_cost_no_exclusions,
        costmlnormcalc::float as likely_phase_cost_normalized,
        costmlnormcumcalc::float as likely_cumulative_phase_cost_normalized,
        costtechlimitcumcalc::float as tech_limit_cumulative_cost,
        costtechlimitnormcalc::float as tech_limit_cost_normalized,
        costtechlimitnormcumcalc::float as tech_limit_cumulative_cost_normalized,

        -- cost per depth (rate conversion: $/meter -> $/foot)
        {{ wv_per_meter_to_per_foot('costperdepthcalc') }} as cost_per_depth_drilled_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthnormcalc') }} as cost_per_depth_drilled_normalized_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthplancalc') }} as planned_cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthplannormcalc') }} as planned_cost_per_depth_normalized_per_ft,

        -- cost variances
        costvariancecalc::float as cost_variance_ml,
        costvariancecumcalc::float as cumulative_cost_variance_ml,
        costvariancemaxcalc::float as cost_variance_max,
        costvariancemaxcumcalc::float as cumulative_cost_variance_max,
        costvariancemincalc::float as cost_variance_min,
        costvariancemincumcalc::float as cumulative_cost_variance_min,
        costvariancetechlimitcalc::float as cost_variance_tech_limit,
        costvariancetechlimitcumcalc::float as cumulative_cost_variance_tech_limit,

        -- mud costs
        phasemudcostcalc::float as phase_mud_cost,
        phasemudcostnormcalc::float as phase_mud_cost_normalized,
        {{ wv_per_meter_to_per_foot('phasemudcostperdepthcalc') }} as phase_mud_cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('phasemudcostperdepthnormcalc') }} as phase_mud_cost_per_depth_normalized_per_ft,

        -- percentages (converted from proportion to percentage)
        pctproblemtimecalc / 0.01 as percent_problem_time,
        pctproblemtimecumcalc / 0.01 as cumulative_percent_problem_time,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,
        ratiodepthactualplancalc / 0.01 as ratio_actual_to_planned_depth_percent,

        -- inclinations
        inclbtmcalc::float as bottom_inclination_degrees,
        inclmaxcalc::float as max_inclination_degrees,
        incltopcalc::float as top_inclination_degrees,

        -- mud density (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('muddensitymaxcalc') }} as max_mud_density_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('muddensitymincalc') }} as min_mud_density_lb_per_gal,
        trim(mudtypcalc)::varchar as mud_type,

        -- volumes (converted from cubic meters to barrels)
        {{ wv_cbm_to_bbl('volkicksumcalc') }} as total_kick_volume_bbl,
        {{ wv_cbm_to_bbl('vollosssumcalc') }} as total_lost_volume_bbl,
        {{ wv_cbm_to_bbl('volmudaddedcalc') }} as mud_added_volume_bbl,
        {{ wv_cbm_to_bbl('volmudaddedlossvarcalc') }} as mud_added_minus_losses_bbl,
        {{ wv_cbm_to_bbl('volmudlosscalc') }} as mud_losses_volume_bbl,

        -- wellbore size (converted from meters to inches)
        {{ wv_meters_to_inches('wellboreszcalc') }} as wellbore_size_inches,

        -- weight (converted from kg to lb)
        {{ wv_kg_to_lb('weightmetalrecovtotalcalc') }} as weight_metal_recovered_total_lb,

        -- day calculations
        dayjobactualcalc::float as actual_job_days,
        dayjobmaxplancalc::float as planned_max_job_days,
        dayjobmaxplanaltcalc::float as planned_max_job_days_alt,
        dayjobminplancalc::float as planned_min_job_days,
        dayjobminplanaltcalc::float as planned_min_job_days_alt,
        dayjobmlplanaltcalc::float as planned_ml_job_days_alt,
        dayjobmlplancalc::float as planned_ml_job_days,
        dayjobtechlimitplancalc::float as planned_tech_limit_job_days,
        dayjobtlplanaltcalc::float as planned_tl_job_days_alt,
        daysfromspudcalc::float as days_from_spud,
        dayspudactualcalc::float as days_from_spud_actual,

        -- planned dates
        dttmendplanmaxcalc::timestamp_ntz as planned_latest_end_date,
        dttmendplanmincalc::timestamp_ntz as planned_earliest_end_date,
        dttmendplanmlcalc::timestamp_ntz as planned_likely_end_date,
        dttmendplantechlimitcalc::timestamp_ntz as planned_tech_limit_end_date,
        dttmstartplanmaxcalc::timestamp_ntz as planned_latest_start_date,
        dttmstartplanmincalc::timestamp_ntz as planned_earliest_start_date,
        dttmstartplanmlcalc::timestamp_ntz as planned_likely_start_date,
        dttmstartplantechlimitcalc::timestamp_ntz as planned_tech_limit_start_date,

        -- activity calculations (depths converted from meters to feet)
        {{ wv_meters_to_feet('activitydepthendmaxcalc') }} as activity_end_depth_max_ft,
        {{ wv_meters_to_feet('activitydepthstartmincalc') }} as activity_start_depth_min_ft,
        activitydurationmaxcalc::float as activity_duration_max_days,
        activitydurationmaxcumcalc::float as activity_duration_max_cumulative_days,
        activitydurationmincalc::float as activity_duration_min_days,
        activitydurationmincumcalc::float as activity_duration_min_cumulative_days,
        activitydurationmlcalc::float as activity_duration_ml_days,
        activitydurationmlcumcalc::float as activity_duration_ml_cumulative_days,

        -- additional cumulative calculations
        durcumactualcalc::float as cumulative_actual_duration_days,
        durcumactualstartphasecalc::float as cumulative_actual_duration_at_phase_start_days,
        durcumflatcalc::float as cumulative_flat_duration_days,
        durcumslopecalc::float as cumulative_slope_duration_days,
        durmlcumnoexcludecalc::float as ml_cumulative_duration_no_exclusions_days,
        durnoprobtimecumdayscalc::float as cumulative_time_log_minus_problem_days,
        {{ wv_days_to_hours('durpersonnelotcumcalc') }} as cumulative_personnel_ot_hours,
        {{ wv_days_to_hours('durpersonnelregcumcalc') }} as cumulative_personnel_regular_hours,
        {{ wv_days_to_hours('durpersonneltotcumcalc') }} as cumulative_personnel_total_hours,
        durproblemtimecumdayscalc::float as cumulative_problem_time_days,
        {{ wv_days_to_hours('durationspudtoplanmaxcalc') }} as duration_spud_to_plan_max_hours,
        {{ wv_days_to_hours('durationspudtoplanmincalc') }} as duration_spud_to_plan_min_hours,
        {{ wv_days_to_hours('durationspudtoplanmlcalc') }} as duration_spud_to_plan_ml_hours,
        {{ wv_days_to_hours('durationspudtoplantechlcalc') }} as duration_spud_to_plan_tech_limit_hours,
        durationtimelogcumspudcalc::float as cumulative_time_log_days_from_spud,
        durationtimelogtotcumcalc::float as cumulative_time_log_total_days,

        -- other fields
        definitive::boolean as is_definitive,
        exclude::boolean as exclude_from_calculations,
        planchange::boolean as is_plan_change,
        trim(planphase)::varchar as plan_details,
        trim(hazards)::varchar as hazards,
        trim(summary)::varchar as summary,
        trim(source)::varchar as source,
        trim(methodtyp1)::varchar as method_type,
        trim(methodtyp2)::varchar as method_subtype,
        trim(flatorslopecalc)::varchar as flat_or_slope,
        trim(formationcalc)::varchar as formation,
        trim(usertxt1)::varchar as user_text_1,

        -- reference fields
        trim(idreclastcascalc)::varchar as last_casing_string_id,
        trim(idreclastcascalctk)::varchar as last_casing_string_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclasttubcalc)::varchar as last_tubing_string_id,
        trim(idreclasttubcalctk)::varchar as last_tubing_string_table_key,

        -- report calculations
        reportdaycalc::int as report_day,
        reportnocalc::int as report_number,
        refnocalc::int as reference_number,

        -- bha and bit calculations
        bhatotalruncalc::int as bha_total_runs,
        bitrevscalc::float as bit_revolutions,

        -- sequence
        sysseq::int as sequence_number,

        -- system locking fields
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as system_tag,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and record_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as job_program_phase_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        job_program_phase_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,
        wellbore_id,
        wellbore_table_key,

        -- phase classification
        phase_type_1,
        phase_type_2,
        phase_type_3,
        phase_type_4,
        combined_phase_types,
        description,

        -- plan information
        planned_likely_duration_days,
        planned_min_duration_days,
        planned_max_duration_days,
        tech_limit_duration_days,
        planned_likely_phase_cost,
        planned_min_phase_cost,
        planned_max_phase_cost,
        tech_limit_cost,

        -- planned depths
        planned_start_depth_ft,
        planned_end_depth_ft,
        planned_start_depth_tvd_ft,
        planned_end_depth_tvd_ft,

        -- actual dates
        actual_start_date,
        actual_end_date,
        derived_end_date,

        -- actual depths
        actual_start_depth_ft,
        actual_end_depth_ft,
        actual_start_depth_tvd_ft,
        actual_end_depth_tvd_ft,
        actual_depth_progress_ft,
        planned_depth_progress_ft,

        -- phase lengths
        actual_phase_length_ft,
        planned_phase_length_ft,

        -- duration calculations (days)
        actual_duration_days,
        duration_variance_days,
        cumulative_duration_variance_days,

        -- duration calculations (hours)
        invisible_lost_time_hours,
        cumulative_invisible_lost_time_hours,
        time_log_minus_problem_hours,
        cumulative_time_log_minus_problem_hours,
        personnel_ot_hours,
        personnel_regular_hours,
        personnel_total_hours,
        problem_time_hours,
        cumulative_problem_time_hours,
        time_log_total_hours,

        -- drilling performance times (hours)
        drilling_time_hours,
        cumulative_drilling_time_hours,
        drilling_time_no_exclusions_hours,
        cumulative_drilling_time_no_exclusions_hours,
        circulating_time_hours,
        cumulative_circulating_time_hours,
        other_time_hours,
        cumulative_other_time_hours,
        tripping_time_hours,
        cumulative_tripping_time_hours,
        rotating_time_hours,
        sliding_time_hours,
        circ_trip_other_time_hours,
        cumulative_circ_trip_other_time_hours,

        -- sensor durations (minutes)
        duration_on_bottom_minutes,
        duration_off_bottom_minutes,
        duration_pipe_moving_minutes,

        -- rate of penetration
        rop_ft_per_hour,
        rop_rotating_ft_per_hour,
        rop_sliding_ft_per_hour,
        rop_instantaneous_avg_min_per_ft,

        -- cost calculations
        actual_phase_field_est,
        actual_phase_cumulative_field_est,
        actual_phase_cumulative_field_est_normalized,
        actual_phase_field_est_normalized,
        max_cumulative_phase_cost,
        max_phase_cost_normalized,
        max_cumulative_phase_cost_normalized,
        min_cumulative_phase_cost,
        min_phase_cost_normalized,
        min_cumulative_phase_cost_normalized,
        likely_cumulative_phase_cost,
        likely_cumulative_phase_cost_no_exclusions,
        likely_phase_cost_normalized,
        likely_cumulative_phase_cost_normalized,
        tech_limit_cumulative_cost,
        tech_limit_cost_normalized,
        tech_limit_cumulative_cost_normalized,

        -- cost per depth
        cost_per_depth_drilled_per_ft,
        cost_per_depth_drilled_normalized_per_ft,
        planned_cost_per_depth_per_ft,
        planned_cost_per_depth_normalized_per_ft,

        -- cost variances
        cost_variance_ml,
        cumulative_cost_variance_ml,
        cost_variance_max,
        cumulative_cost_variance_max,
        cost_variance_min,
        cumulative_cost_variance_min,
        cost_variance_tech_limit,
        cumulative_cost_variance_tech_limit,

        -- mud costs
        phase_mud_cost,
        phase_mud_cost_normalized,
        phase_mud_cost_per_depth_per_ft,
        phase_mud_cost_per_depth_normalized_per_ft,

        -- percentages
        percent_problem_time,
        cumulative_percent_problem_time,
        percent_time_rotating,
        percent_time_sliding,
        ratio_actual_to_planned_depth_percent,

        -- inclinations
        bottom_inclination_degrees,
        max_inclination_degrees,
        top_inclination_degrees,

        -- mud density
        max_mud_density_lb_per_gal,
        min_mud_density_lb_per_gal,
        mud_type,

        -- volumes
        total_kick_volume_bbl,
        total_lost_volume_bbl,
        mud_added_volume_bbl,
        mud_added_minus_losses_bbl,
        mud_losses_volume_bbl,

        -- wellbore size
        wellbore_size_inches,

        -- weight
        weight_metal_recovered_total_lb,

        -- day calculations
        actual_job_days,
        planned_max_job_days,
        planned_max_job_days_alt,
        planned_min_job_days,
        planned_min_job_days_alt,
        planned_ml_job_days_alt,
        planned_ml_job_days,
        planned_tech_limit_job_days,
        planned_tl_job_days_alt,
        days_from_spud,
        days_from_spud_actual,

        -- planned dates
        planned_latest_end_date,
        planned_earliest_end_date,
        planned_likely_end_date,
        planned_tech_limit_end_date,
        planned_latest_start_date,
        planned_earliest_start_date,
        planned_likely_start_date,
        planned_tech_limit_start_date,

        -- activity calculations
        activity_end_depth_max_ft,
        activity_start_depth_min_ft,
        activity_duration_max_days,
        activity_duration_max_cumulative_days,
        activity_duration_min_days,
        activity_duration_min_cumulative_days,
        activity_duration_ml_days,
        activity_duration_ml_cumulative_days,

        -- additional cumulative calculations
        cumulative_actual_duration_days,
        cumulative_actual_duration_at_phase_start_days,
        cumulative_flat_duration_days,
        cumulative_slope_duration_days,
        ml_cumulative_duration_no_exclusions_days,
        cumulative_time_log_minus_problem_days,
        cumulative_personnel_ot_hours,
        cumulative_personnel_regular_hours,
        cumulative_personnel_total_hours,
        cumulative_problem_time_days,
        duration_spud_to_plan_max_hours,
        duration_spud_to_plan_min_hours,
        duration_spud_to_plan_ml_hours,
        duration_spud_to_plan_tech_limit_hours,
        cumulative_time_log_days_from_spud,
        cumulative_time_log_total_days,

        -- flags
        is_definitive,
        exclude_from_calculations,
        is_plan_change,

        -- descriptive fields
        plan_details,
        hazards,
        summary,
        source,
        method_type,
        method_subtype,
        flat_or_slope,
        formation,
        user_text_1,

        -- reference fields
        last_casing_string_id,
        last_casing_string_table_key,
        last_rig_id,
        last_rig_table_key,
        last_tubing_string_id,
        last_tubing_string_table_key,

        -- report calculations
        report_day,
        report_number,
        reference_number,

        -- bha and bit calculations
        bha_total_runs,
        bit_revolutions,

        -- sequence
        sequence_number,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
