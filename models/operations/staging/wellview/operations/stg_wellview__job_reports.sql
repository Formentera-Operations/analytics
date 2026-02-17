{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBREPORT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as report_id,
        trim(idrecparent)::varchar as job_id,
        trim(idwell)::varchar as well_id,

        -- report period
        dttmstart::timestamp_ntz as report_start_datetime,
        dttmend::timestamp_ntz as report_end_datetime,
        reportnocalc::float as report_number,
        reportdaycalc::float as report_day,
        daysfromspudcalc::float as days_from_spud,
        daysfromspudtorrcalc::float as days_from_spud_to_rr,

        -- depths (meters -> feet)
        {{ wv_meters_to_feet('depthenddpcalc') }} as end_depth_ft,
        {{ wv_meters_to_feet('depthstartdpcalc') }} as start_depth_ft,
        {{ wv_meters_to_feet('depthprogressdpcalc') }} as depth_progress_ft,
        {{ wv_meters_to_feet('depthnetprogressdpcalc') }} as net_depth_progress_ft,
        {{ wv_meters_to_feet('depthrotatingcalc') }} as depth_rotating_ft,
        {{ wv_meters_to_feet('depthslidingcalc') }} as depth_sliding_ft,
        {{ wv_meters_to_feet('depthtvdenddpcalc') }} as end_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdstartdpcalc') }} as start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthenddpcumcalc') }} as cumulative_end_depth_ft,
        {{ wv_meters_to_feet('depthenddpnullcalc') }} as end_depth_null_ft,
        {{ wv_meters_to_feet('depthstartdpnullcalc') }} as start_depth_null_ft,

        -- drilling rates (meters/sec -> ft/hr)
        {{ wv_mps_to_ft_per_hr('ropcalc') }} as rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('roprotatingcalc') }} as rop_rotating_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropslidingcalc') }} as rop_sliding_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('depthperdurcalc') }} as depth_per_duration_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('depthperdurvarcalc') }} as depth_per_duration_variance_ft_per_hr,

        -- time durations (days -> hours)
        {{ wv_days_to_hours('tmdrillcalc') }} as drilling_time_hours,
        {{ wv_days_to_hours('tmrotatingcalc') }} as rotating_time_hours,
        {{ wv_days_to_hours('tmslidingcalc') }} as sliding_time_hours,
        {{ wv_days_to_hours('tmcirccalc') }} as circulating_time_hours,
        {{ wv_days_to_hours('tmtripcalc') }} as tripping_time_hours,
        {{ wv_days_to_hours('tmothercalc') }} as other_time_hours,
        {{ wv_days_to_hours('durationtimelogtotalcalc') }} as total_time_log_hours,
        {{ wv_days_to_hours('durationnoprobtimecalc') }} as no_problem_time_hours,
        {{ wv_days_to_hours('durationproblemtimecalc') }} as problem_time_hours,
        {{ wv_days_to_hours('rigtime') }} as rig_time_hours,

        -- cumulative time durations (days -> hours)
        {{ wv_days_to_hours('tmdrillcumcalc') }} as cumulative_drilling_time_hours,
        {{ wv_days_to_hours('tmcirccumcalc') }} as cumulative_circulating_time_hours,
        {{ wv_days_to_hours('tmtripcumcalc') }} as cumulative_tripping_time_hours,
        {{ wv_days_to_hours('tmothercumcalc') }} as cumulative_other_time_hours,
        {{ wv_days_to_hours('tmcirctripothercalc') }} as circ_trip_other_time_hours,
        {{ wv_days_to_hours('tmcirctripothercumcalc') }} as cumulative_circ_trip_other_hours,
        {{ wv_days_to_hours('tmdrillnoexccalc') }} as drilling_time_no_exclude_hours,
        {{ wv_days_to_hours('tmdrillcumnoexccalc') }} as cumulative_drilling_no_exclude_hours,
        {{ wv_days_to_hours('durationnoprobtimecumcalc') }} as cumulative_no_problem_time_hours,
        {{ wv_days_to_hours('durationproblemtimecumcalc') }} as cumulative_problem_time_hours,
        {{ wv_days_to_hours('rigtimecumcalc') }} as cumulative_rig_time_hours,

        -- personnel hours (days -> hours)
        {{ wv_days_to_hours('durationpersonnelregcalc') }} as personnel_regular_hours,
        {{ wv_days_to_hours('durationpersonnelotcalc') }} as personnel_overtime_hours,
        {{ wv_days_to_hours('durationpersonneltotcalc') }} as personnel_total_hours,
        {{ wv_days_to_hours('durpersonnelregcumcalc') }} as cumulative_personnel_regular_hours,
        {{ wv_days_to_hours('durpersonnelotcumcalc') }} as cumulative_personnel_overtime_hours,
        {{ wv_days_to_hours('durpersonneltotcumcalc') }} as cumulative_personnel_total_hours,

        -- duration in days (no conversion)
        rigdayscalc::float as rig_days,
        rigdayscumcalc::float as cumulative_rig_days,
        durstarttoendcalc::float as duration_start_to_end_days,
        durprojectedmljobcalc::float as projected_ml_job_duration_days,
        durprojectedminjobcalc::float as projected_min_job_duration_days,
        durprojectedmaxjobcalc::float as projected_max_job_duration_days,
        durprojectedtljobcalc::float as projected_tl_job_duration_days,
        durprojectedmlphasecalc::float as projected_ml_phase_duration_days,
        durprojectedminphasecalc::float as projected_min_phase_duration_days,
        durprojectedmaxphasecalc::float as projected_max_phase_duration_days,
        durprojectedtlphasecalc::float as projected_tl_phase_duration_days,
        durationtimelogcum12hrcalc::float as time_log_cumulative_12hr_days,
        durationtimelogcumspudcalc::float as time_log_cumulative_spud_days,
        durationtimelogcumspudrrcalc::float as time_log_cumulative_spud_rr_days,
        durationtimelogtotcumcalc::float as total_time_log_cumulative_days,
        durlastsinccalc::float as duration_last_since_days,
        durlastsincreportcalc::float as duration_last_since_report_days,
        durlastsincrptdaycalc::float as duration_last_since_report_day_days,
        durlastsincreportrptdaycalc::float as duration_last_since_report_report_day_days,
        durnoprobtimecumdayscalc::float as cumulative_no_problem_time_days,
        durproblemtimecumdayscalc::float as cumulative_problem_time_days,
        durationsinceltinc::float as duration_since_lti_days,
        durationsincerptinc::float as duration_since_reportable_incident_days,

        -- time ahead/behind schedule (days)
        timeaheadmljobcalc::float as time_ahead_ml_job_days,
        timeaheadminjobcalc::float as time_ahead_min_job_days,
        timeaheadmaxjobcalc::float as time_ahead_max_job_days,
        timeaheadtljobcalc::float as time_ahead_tl_job_days,
        timeaheadmlphasecalc::float as time_ahead_ml_phase_days,
        timeaheadminphasecalc::float as time_ahead_min_phase_days,
        timeaheadmaxphasecalc::float as time_ahead_max_phase_days,
        timeaheadtlphasecalc::float as time_ahead_tl_phase_days,

        -- short duration activities (days -> minutes)
        {{ wv_days_to_minutes('duroffbtmcalc') }} as duration_off_bottom_minutes,
        {{ wv_days_to_minutes('duronbtmcalc') }} as duration_on_bottom_minutes,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as duration_pipe_moving_minutes,

        -- bha and bit performance
        bhatotalruncalc::float as bha_total_runs,
        bitrevscalc::float as bit_revolutions,

        -- cost per unit (rate conversion: $/meter -> $/foot)
        {{ wv_per_meter_to_per_foot('costperdepthcalc') }} as cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthcumcalc') }} as cumulative_cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthvarcalc') }} as cost_per_depth_variance_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthnormcalc') }} as cost_per_depth_normalized_per_ft,
        costpertldurcalc / 24 as cost_per_hour,
        costpertldurnormcalc / 24 as cost_per_hour_normalized,
        {{ wv_per_meter_to_per_foot('mudcostperdepthcalc') }} as mud_cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('mudcostperdepthcumcalc') }} as cumulative_mud_cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('mudcostperdepthnormcalc') }} as mud_cost_per_depth_normalized_per_ft,
        {{ wv_per_meter_to_per_foot('mudcostperdepthcumnormcalc') }} as cumulative_mud_cost_per_depth_normalized_per_ft,

        -- total costs (no conversion)
        costtodatecalc::float as cost_to_date,
        costtodatenormcalc::float as cost_to_date_normalized,
        costtotalcalc::float as total_cost,
        costtotalnormcalc::float as total_cost_normalized,
        costjobsupplyamtcalc::float as job_supply_cost,
        costjobsupplyamtnormcalc::float as job_supply_cost_normalized,
        costjobsupplyamttodatecalc::float as job_supply_cost_to_date,
        costjobsupplyamttodtncalc::float as job_supply_cost_to_date_normalized,
        costmudaddcalc::float as mud_additive_cost,
        costmudaddnormcalc::float as mud_additive_cost_normalized,
        costmudaddtodatecalc::float as mud_additive_cost_to_date,
        costmudaddtodatenormcalc::float as mud_additive_cost_to_date_normalized,

        -- projected costs (no conversion)
        costprojectedmljobcalc::float as projected_ml_job_cost,
        costprojectedminjobcalc::float as projected_min_job_cost,
        costprojectedmaxjobcalc::float as projected_max_job_cost,
        costprojectedtljobcalc::float as projected_tl_job_cost,
        costprojectedmljobnormcalc::float as projected_ml_job_cost_normalized,
        costprojectedminjobnormcalc::float as projected_min_job_cost_normalized,
        costprojectedmaxjobnormcalc::float as projected_max_job_cost_normalized,
        costprojectedtljobnormcalc::float as projected_tl_job_cost_normalized,
        costprojectedmlphasecalc::float as projected_ml_phase_cost,
        costprojectedminphasecalc::float as projected_min_phase_cost,
        costprojectedmaxphasecalc::float as projected_max_phase_cost,
        costprojectedtlphasecalc::float as projected_tl_phase_cost,
        costprojectedmlphasenormcalc::float as projected_ml_phase_cost_normalized,
        costprojectedminphasenormcalc::float as projected_min_phase_cost_normalized,
        costprojectedmaxphasenormcalc::float as projected_max_phase_cost_normalized,
        costprojectedtlphasenormcalc::float as projected_tl_phase_cost_normalized,

        -- cost variances (no conversion)
        costforecastfieldvarcalc::float as cost_forecast_field_variance,
        costnormforecastfieldvarcalc::float as cost_normalized_forecast_field_variance,

        -- mud density (kg/m3 -> ppg)
        {{ wv_kgm3_to_lb_per_gal('lastmuddensitycalc') }} as last_mud_density_ppg,

        -- volumes (cubic meters -> barrels)
        {{ wv_cbm_to_bbl('volholecalc') }} as hole_volume_bbl,
        {{ wv_cbm_to_bbl('volmudactivecalc') }} as active_mud_volume_bbl,
        {{ wv_cbm_to_bbl('volmudactivevarcalc') }} as active_mud_volume_variance_bbl,
        {{ wv_cbm_to_bbl('volmudbalancecalc') }} as mud_balance_volume_bbl,
        {{ wv_cbm_to_bbl('volholevarcalc') }} as hole_volume_variance_bbl,
        {{ wv_cbm_to_bbl('volmudaddedcalc') }} as mud_volume_added_bbl,
        {{ wv_cbm_to_bbl('volmudaddedcumcalc') }} as cumulative_mud_volume_added_bbl,
        {{ wv_cbm_to_bbl('volmudlosscalc') }} as mud_volume_lost_bbl,
        {{ wv_cbm_to_bbl('volmudlosscumcalc') }} as cumulative_mud_volume_lost_bbl,
        {{ wv_cbm_to_bbl('volmudaddedlossvarcalc') }} as mud_added_loss_variance_bbl,
        {{ wv_cbm_to_bbl('volmudaddedlossvarcumcalc') }} as cumulative_mud_added_loss_variance_bbl,
        {{ wv_cbm_to_bbl('volmudtankcalc') }} as mud_tank_volume_bbl,
        {{ wv_cbm_to_bbl('volbittoshoecalc') }} as bit_to_shoe_volume_bbl,
        {{ wv_cbm_to_bbl('volcastoptorisertopcalc') }} as casing_top_to_riser_top_volume_bbl,
        {{ wv_cbm_to_bbl('volpumptobitcalc') }} as pump_to_bit_volume_bbl,
        {{ wv_cbm_to_bbl('volshoetocastopcalc') }} as shoe_to_casing_top_volume_bbl,

        -- gas readings (proportion -> percentage)
        gasbackgroundavg / 0.01 as background_gas_avg_percent,
        gasbackgroundmax / 0.01 as background_gas_max_percent,
        gasdrillavg / 0.01 as drilling_gas_avg_percent,
        gasdrillmax / 0.01 as drilling_gas_max_percent,
        gasconnectionavg / 0.01 as connection_gas_avg_percent,
        gasconnectionmax / 0.01 as connection_gas_max_percent,
        gastripavg / 0.01 as trip_gas_avg_percent,
        gastripmax / 0.01 as trip_gas_max_percent,

        -- h2s (proportion -> ppm)
        h2smax / 1E-06 as h2s_max_ppm,

        -- percentages (proportion -> percentage)
        pctproblemtimecalc / 0.01 as problem_time_percentage,
        pctproblemtimecumcalc / 0.01 as cumulative_problem_time_percentage,
        percentfieldafecalc / 0.01 as percent_field_afe,
        percentcompletemljobcalc / 0.01 as percent_complete_ml_job,
        percentcompleteminjobcalc / 0.01 as percent_complete_min_job,
        percentcompletemaxjobcalc / 0.01 as percent_complete_max_job,
        percentcompletetljobcalc / 0.01 as percent_complete_tl_job,
        percentcompletemlphasecalc / 0.01 as percent_complete_ml_phase,
        percentcompleteminphasecalc / 0.01 as percent_complete_min_phase,
        percentcompletemaxphasecalc / 0.01 as percent_complete_max_phase,
        percentcompletetlphasecalc / 0.01 as percent_complete_tl_phase,
        percentdepthrotatingcalc / 0.01 as percent_depth_rotating,
        percentdepthslidingcalc / 0.01 as percent_depth_sliding,
        percenttmrotatingcalc / 0.01 as percent_time_rotating,
        percenttmslidingcalc / 0.01 as percent_time_sliding,
        ratiodurprojmlplancalc / 0.01 as ratio_duration_projected_ml_plan_percent,
        ratiodurprojminplancalc / 0.01 as ratio_duration_projected_min_plan_percent,
        ratiodurprojmaxplancalc / 0.01 as ratio_duration_projected_max_plan_percent,
        ratiodurprojtlplancalc / 0.01 as ratio_duration_projected_tl_plan_percent,

        -- safety metrics
        safetyincnocalc::float as safety_incident_count,
        safetyinccalc::float as safety_incident_calc,
        safetyincnocumcalc::float as cumulative_safety_incident_count,
        safetyincratecalc::float as safety_incident_rate,
        safetyincreportcalc::float as safety_incident_report_calc,
        safetyincreportnocalc::float as reportable_safety_incident_count,
        safetyincreportnocumcalc::float as cumulative_reportable_safety_incident_count,
        safetyincreportratecalc::float as reportable_safety_incident_rate,
        hazardidnorptcalc::float as hazard_id_report_count,
        hazardidnorptcumcalc::float as cumulative_hazard_id_report_count,

        -- personnel
        headcountcalc::float as head_count,

        -- environmental conditions
        condtemp / 0.555555555555556 + 32 as temperature_fahrenheit,
        trim(condhole)::varchar as hole_conditions,
        trim(condlease)::varchar as lease_conditions,
        trim(condroad)::varchar as road_conditions,
        trim(condwave)::varchar as wave_conditions,
        trim(condweather)::varchar as weather_conditions,
        trim(condwind)::varchar as wind_conditions,

        -- status and operational info
        trim(statusend)::varchar as report_status_end,
        trim(summaryops)::varchar as operations_summary,
        trim(remarks)::varchar as remarks,
        trim(contactcalc)::varchar as contact_calc,

        -- planning and next operations
        trim(plannextrptops)::varchar as planned_next_report_ops,
        trim(rpttmactops)::varchar as report_time_actual_ops,

        -- time log codes
        trim(timelogcode1calc)::varchar as time_log_code_1,
        trim(timelogcode2calc)::varchar as time_log_code_2,
        trim(timelogcode3calc)::varchar as time_log_code_3,
        trim(timelogcode4calc)::varchar as time_log_code_4,

        -- projected end dates
        dttmprojendmljobcalc::timestamp_ntz as projected_end_date_ml_job,
        dttmprojendminjobcalc::timestamp_ntz as projected_end_date_min_job,
        dttmprojendmaxjobcalc::timestamp_ntz as projected_end_date_max_job,
        dttmprojendtljobcalc::timestamp_ntz as projected_end_date_tl_job,
        dttmprojendmlphasecalc::timestamp_ntz as projected_end_date_ml_phase,
        dttmprojendminphasecalc::timestamp_ntz as projected_end_date_min_phase,
        dttmprojendmaxphasecalc::timestamp_ntz as projected_end_date_max_phase,
        dttmprojendtlphasecalc::timestamp_ntz as projected_end_date_tl_phase,

        -- depth projection method
        trim(depthtvdendprojmethod)::varchar as depth_tvd_end_projection_method,

        -- lesson and problem indicators
        intlessoncalc::float as lessons_learned_indicator,
        intproblemcalc::float as problems_indicator,

        -- rig information
        trim(rigscalc)::varchar as rigs_calc,

        -- weight (kg -> lbs)
        {{ wv_kg_to_lb('weightmetalrecovtotalcalc') }} as total_metal_recovery_weight_lbs,

        -- foreign keys
        trim(idrecwellborecalc)::varchar as wellbore_id,
        trim(idrecwellborecalctk)::varchar as wellbore_table_key,
        trim(idrecjobprogramphasecalc)::varchar as job_program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as job_program_phase_table_key,
        trim(idreclastcascalc)::varchar as last_casing_id,
        trim(idreclastcascalctk)::varchar as last_casing_table_key,
        trim(idrecnextcas)::varchar as next_casing_id,
        trim(idrecnextcastk)::varchar as next_casing_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,

        -- user fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        trim(usertxt4)::varchar as user_text_4,
        trim(usertxt5)::varchar as user_text_5,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,
        usernum4::float as user_number_4,
        usernum5::float as user_number_5,
        userboolean1::boolean as user_boolean_1,
        userboolean2::boolean as user_boolean_2,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

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
        and report_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['report_id']) }} as job_report_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        job_report_sk,

        -- identifiers
        report_id,
        job_id,
        well_id,

        -- report period
        report_start_datetime,
        report_end_datetime,
        report_number,
        report_day,
        days_from_spud,
        days_from_spud_to_rr,

        -- depths
        end_depth_ft,
        start_depth_ft,
        depth_progress_ft,
        net_depth_progress_ft,
        depth_rotating_ft,
        depth_sliding_ft,
        end_depth_tvd_ft,
        start_depth_tvd_ft,
        cumulative_end_depth_ft,
        end_depth_null_ft,
        start_depth_null_ft,

        -- drilling rates
        rop_ft_per_hr,
        rop_rotating_ft_per_hr,
        rop_sliding_ft_per_hr,
        depth_per_duration_ft_per_hr,
        depth_per_duration_variance_ft_per_hr,

        -- time durations
        drilling_time_hours,
        rotating_time_hours,
        sliding_time_hours,
        circulating_time_hours,
        tripping_time_hours,
        other_time_hours,
        total_time_log_hours,
        no_problem_time_hours,
        problem_time_hours,
        rig_time_hours,

        -- cumulative time durations
        cumulative_drilling_time_hours,
        cumulative_circulating_time_hours,
        cumulative_tripping_time_hours,
        cumulative_other_time_hours,
        circ_trip_other_time_hours,
        cumulative_circ_trip_other_hours,
        drilling_time_no_exclude_hours,
        cumulative_drilling_no_exclude_hours,
        cumulative_no_problem_time_hours,
        cumulative_problem_time_hours,
        cumulative_rig_time_hours,

        -- personnel hours
        personnel_regular_hours,
        personnel_overtime_hours,
        personnel_total_hours,
        cumulative_personnel_regular_hours,
        cumulative_personnel_overtime_hours,
        cumulative_personnel_total_hours,

        -- duration in days
        rig_days,
        cumulative_rig_days,
        duration_start_to_end_days,
        projected_ml_job_duration_days,
        projected_min_job_duration_days,
        projected_max_job_duration_days,
        projected_tl_job_duration_days,
        projected_ml_phase_duration_days,
        projected_min_phase_duration_days,
        projected_max_phase_duration_days,
        projected_tl_phase_duration_days,
        time_log_cumulative_12hr_days,
        time_log_cumulative_spud_days,
        time_log_cumulative_spud_rr_days,
        total_time_log_cumulative_days,
        duration_last_since_days,
        duration_last_since_report_days,
        duration_last_since_report_day_days,
        duration_last_since_report_report_day_days,
        cumulative_no_problem_time_days,
        cumulative_problem_time_days,
        duration_since_lti_days,
        duration_since_reportable_incident_days,

        -- time ahead/behind schedule
        time_ahead_ml_job_days,
        time_ahead_min_job_days,
        time_ahead_max_job_days,
        time_ahead_tl_job_days,
        time_ahead_ml_phase_days,
        time_ahead_min_phase_days,
        time_ahead_max_phase_days,
        time_ahead_tl_phase_days,

        -- short duration activities
        duration_off_bottom_minutes,
        duration_on_bottom_minutes,
        duration_pipe_moving_minutes,

        -- bha and bit performance
        bha_total_runs,
        bit_revolutions,

        -- cost per unit
        cost_per_depth_per_ft,
        cumulative_cost_per_depth_per_ft,
        cost_per_depth_variance_per_ft,
        cost_per_depth_normalized_per_ft,
        cost_per_hour,
        cost_per_hour_normalized,
        mud_cost_per_depth_per_ft,
        cumulative_mud_cost_per_depth_per_ft,
        mud_cost_per_depth_normalized_per_ft,
        cumulative_mud_cost_per_depth_normalized_per_ft,

        -- total costs
        cost_to_date,
        cost_to_date_normalized,
        total_cost,
        total_cost_normalized,
        job_supply_cost,
        job_supply_cost_normalized,
        job_supply_cost_to_date,
        job_supply_cost_to_date_normalized,
        mud_additive_cost,
        mud_additive_cost_normalized,
        mud_additive_cost_to_date,
        mud_additive_cost_to_date_normalized,

        -- projected costs
        projected_ml_job_cost,
        projected_min_job_cost,
        projected_max_job_cost,
        projected_tl_job_cost,
        projected_ml_job_cost_normalized,
        projected_min_job_cost_normalized,
        projected_max_job_cost_normalized,
        projected_tl_job_cost_normalized,
        projected_ml_phase_cost,
        projected_min_phase_cost,
        projected_max_phase_cost,
        projected_tl_phase_cost,
        projected_ml_phase_cost_normalized,
        projected_min_phase_cost_normalized,
        projected_max_phase_cost_normalized,
        projected_tl_phase_cost_normalized,

        -- cost variances
        cost_forecast_field_variance,
        cost_normalized_forecast_field_variance,

        -- mud density
        last_mud_density_ppg,

        -- volumes
        hole_volume_bbl,
        active_mud_volume_bbl,
        active_mud_volume_variance_bbl,
        mud_balance_volume_bbl,
        hole_volume_variance_bbl,
        mud_volume_added_bbl,
        cumulative_mud_volume_added_bbl,
        mud_volume_lost_bbl,
        cumulative_mud_volume_lost_bbl,
        mud_added_loss_variance_bbl,
        cumulative_mud_added_loss_variance_bbl,
        mud_tank_volume_bbl,
        bit_to_shoe_volume_bbl,
        casing_top_to_riser_top_volume_bbl,
        pump_to_bit_volume_bbl,
        shoe_to_casing_top_volume_bbl,

        -- gas readings
        background_gas_avg_percent,
        background_gas_max_percent,
        drilling_gas_avg_percent,
        drilling_gas_max_percent,
        connection_gas_avg_percent,
        connection_gas_max_percent,
        trip_gas_avg_percent,
        trip_gas_max_percent,

        -- h2s
        h2s_max_ppm,

        -- percentages
        problem_time_percentage,
        cumulative_problem_time_percentage,
        percent_field_afe,
        percent_complete_ml_job,
        percent_complete_min_job,
        percent_complete_max_job,
        percent_complete_tl_job,
        percent_complete_ml_phase,
        percent_complete_min_phase,
        percent_complete_max_phase,
        percent_complete_tl_phase,
        percent_depth_rotating,
        percent_depth_sliding,
        percent_time_rotating,
        percent_time_sliding,
        ratio_duration_projected_ml_plan_percent,
        ratio_duration_projected_min_plan_percent,
        ratio_duration_projected_max_plan_percent,
        ratio_duration_projected_tl_plan_percent,

        -- safety metrics
        safety_incident_count,
        safety_incident_calc,
        cumulative_safety_incident_count,
        safety_incident_rate,
        safety_incident_report_calc,
        reportable_safety_incident_count,
        cumulative_reportable_safety_incident_count,
        reportable_safety_incident_rate,
        hazard_id_report_count,
        cumulative_hazard_id_report_count,

        -- personnel
        head_count,

        -- environmental conditions
        temperature_fahrenheit,
        hole_conditions,
        lease_conditions,
        road_conditions,
        wave_conditions,
        weather_conditions,
        wind_conditions,

        -- status and operational info
        report_status_end,
        operations_summary,
        remarks,
        contact_calc,

        -- planning and next operations
        planned_next_report_ops,
        report_time_actual_ops,

        -- time log codes
        time_log_code_1,
        time_log_code_2,
        time_log_code_3,
        time_log_code_4,

        -- projected end dates
        projected_end_date_ml_job,
        projected_end_date_min_job,
        projected_end_date_max_job,
        projected_end_date_tl_job,
        projected_end_date_ml_phase,
        projected_end_date_min_phase,
        projected_end_date_max_phase,
        projected_end_date_tl_phase,

        -- depth projection method
        depth_tvd_end_projection_method,

        -- lesson and problem indicators
        lessons_learned_indicator,
        problems_indicator,

        -- rig information
        rigs_calc,

        -- weight
        total_metal_recovery_weight_lbs,

        -- foreign keys
        wellbore_id,
        wellbore_table_key,
        job_program_phase_id,
        job_program_phase_table_key,
        last_casing_id,
        last_casing_table_key,
        next_casing_id,
        next_casing_table_key,
        last_rig_id,
        last_rig_table_key,

        -- user fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_text_4,
        user_text_5,
        user_number_1,
        user_number_2,
        user_number_3,
        user_number_4,
        user_number_5,
        user_boolean_1,
        user_boolean_2,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
