{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran deduplication on job PK
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOB') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, unit conversions. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as job_id,
        trim(idwell)::varchar as well_id,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,

        -- job classification
        trim(wvtyp)::varchar as job_category,
        trim(jobtyp)::varchar as job_type_primary,
        trim(jobsubtyp)::varchar as job_type_secondary,
        complexityindex::float as complexity_index,

        -- key dates
        dttmstart::timestamp_ntz as job_start_at,
        dttmend::timestamp_ntz as job_end_at,
        dttmspud::timestamp_ntz as spud_at,
        dttmstartplan::timestamp_ntz as planned_start_at,
        dttmendcalc::timestamp_ntz as calculated_end_at,
        dttmtotaldepthcalc::timestamp_ntz as total_depth_reached_at,

        -- planned dates
        dttmendplanmlcalc::timestamp_ntz as planned_end_ml_at,
        dttmendplanmincalc::timestamp_ntz as planned_end_min_at,
        dttmendplanmaxcalc::timestamp_ntz as planned_end_max_at,
        dttmendplantechlimitcalc::timestamp_ntz as planned_end_tech_limit_at,

        -- objectives and targets
        trim(objective)::varchar as job_objective,
        trim(objectivegeo)::varchar as geological_objective,
        trim(targetform)::varchar as target_formation,

        -- depths (metric → feet)
        {{ wv_meters_to_feet('targetdepth') }} as target_depth_ft,
        {{ wv_meters_to_feet('targetdepthtvdcalc') }} as target_depth_tvd_ft,
        {{ wv_meters_to_feet('totaldepthcalc') }} as total_depth_ft,
        {{ wv_meters_to_feet('totaldepthtvdcalc') }} as total_depth_tvd_ft,
        {{ wv_meters_to_feet('depthdrilledcalc') }} as depth_drilled_ft,
        {{ wv_meters_to_feet('depthrotatingcalc') }} as depth_rotating_ft,
        {{ wv_meters_to_feet('depthslidingcalc') }} as depth_sliding_ft,
        {{ wv_meters_to_feet('depthplanmaxcalc') }} as planned_max_depth_ft,
        {{ wv_meters_to_feet('depthdrilledperbhacalc') }} as depth_drilled_per_bha_ft,
        {{ wv_meters_to_feet('depthdrilledperreportnocalc') }} as depth_drilled_per_report_ft,
        {{ wv_meters_to_feet('depthperratiodurationcalc') }} as depth_per_ratio_duration_ft,
        {{ wv_meters_to_feet('tdtomudcalc') }} as td_to_mud_line_ft,

        -- drilling rates (metric → ft/hr)
        {{ wv_mps_to_ft_per_hr('ropcalc') }} as rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropavgfromspudcalc') }} as rop_avg_from_spud_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('roprotatingcalc') }} as rop_rotating_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropslidingcalc') }} as rop_sliding_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropspudtimelogcalc') }} as rop_spud_time_log_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('roptimelogcalc') }} as rop_time_log_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('depthdrilledspudtorrcalc') }} as depth_drilled_spud_to_rr_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('depthperdurplanmlcalc') }} as planned_ml_rate_ft_per_hr,

        -- time durations — days (no conversion)
        durationiltcalc::float as duration_ilt_days,
        durationmaxtotalcalc::float as duration_max_total_days,
        durationmintotalcalc::float as duration_min_total_days,
        durationmltotalcalc::float as duration_ml_total_days,
        durstarttoendcalc::float as duration_start_to_end_days,
        durationtechlimittotalcalc::float as duration_tech_limit_total_days,
        durmltotalnoplanchangecalc::float as duration_ml_no_plan_change_days,
        durmlnoexcludecalc::float as duration_ml_no_exclude_days,

        -- time durations — hours (days → hours)
        {{ wv_days_to_hours('tmdrillcalc') }} as drilling_time_hours,
        {{ wv_days_to_hours('tmrotatingcalc') }} as rotating_time_hours,
        {{ wv_days_to_hours('tmslidingcalc') }} as sliding_time_hours,
        {{ wv_days_to_hours('tmcirccalc') }} as circulating_time_hours,
        {{ wv_days_to_hours('tmtripcalc') }} as tripping_time_hours,
        {{ wv_days_to_hours('tmothercalc') }} as other_time_hours,
        {{ wv_days_to_hours('durationtimelogtotalcalc') }} as time_log_total_hours,
        {{ wv_days_to_hours('durationspudtimelogcalc') }} as spud_time_log_hours,
        {{ wv_days_to_hours('durationspudtotdcalc') }} as spud_to_td_hours,
        {{ wv_days_to_hours('durationspudtorrcalc') }} as spud_to_rr_hours,
        {{ wv_days_to_hours('durationnoproblemtimecalc') }} as no_problem_time_hours,
        {{ wv_days_to_hours('durationproblemtimecalc') }} as problem_time_hours,
        {{ wv_days_to_hours('durationpersonnelregcalc') }} as personnel_regular_hours,
        {{ wv_days_to_hours('durationpersonnelotcalc') }} as personnel_ot_hours,
        {{ wv_days_to_hours('durationpersonneltotcalc') }} as personnel_total_hours,
        {{ wv_days_to_hours('durationspudtoplanmlcalc') }} as spud_to_plan_ml_hours,
        {{ wv_days_to_hours('durationspudtoplanmincalc') }} as spud_to_plan_min_hours,
        {{ wv_days_to_hours('durationspudtoplanmaxcalc') }} as spud_to_plan_max_hours,
        {{ wv_days_to_hours('durationspudtoplantechlimcalc') }} as spud_to_plan_tech_limit_hours,
        {{ wv_days_to_hours('estproblemtimecalc') }} as estimated_problem_time_hours,
        {{ wv_days_to_hours('esttimesavecalc') }} as estimated_time_savings_hours,

        -- time durations — minutes (days → minutes)
        {{ wv_days_to_minutes('duroffbtmcalc') }} as duration_off_bottom_minutes,
        {{ wv_days_to_minutes('duronbtmcalc') }} as duration_on_bottom_minutes,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as duration_pipe_moving_minutes,

        -- afe information
        trim(afenumbercalc)::varchar as afe_number,
        trim(afenumbersuppcalc)::varchar as afe_supplemental_number,
        afeamtcalc::float as afe_amount,
        afeamtnormcalc::float as afe_amount_normalized,
        afesupamtcalc::float as afe_supplemental_amount,
        afesupamtnormcalc::float as afe_supplemental_amount_normalized,
        afetotalcalc::float as afe_total_amount,
        afetotalnormcalc::float as afe_total_amount_normalized,
        trim(afecosttypcalc)::varchar as afe_cost_type,

        -- cost per hour (days → hours: / 24)
        afeperdurmlcalc / 24 as afe_per_hour,
        afeperdurmlnormcalc / 24 as afe_per_hour_normalized,
        costpertldurcalc / 24 as cost_per_hour,
        costpertldurnormcalc / 24 as cost_per_hour_normalized,

        -- cost per foot (rate conversion: $/meter -> $/foot)
        {{ wv_per_meter_to_per_foot('afepertargetdepthcalc') }} as afe_per_target_depth_per_ft,
        {{ wv_per_meter_to_per_foot('afepertargetdepthnormcalc') }} as afe_per_target_depth_normalized_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthcalc') }} as cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('costnormperdepthcalc') }} as cost_normalized_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('costperdepthplanmlcalc') }} as cost_per_depth_plan_ml_per_ft,
        {{ wv_per_meter_to_per_foot('costnormperdepthplanmlcalc') }} as cost_normalized_per_depth_plan_ml_per_ft,
        {{ wv_per_meter_to_per_foot('costperlateralcalc') }} as cost_per_lateral_per_ft,
        {{ wv_per_meter_to_per_foot('costperlateralallcalc') }} as cost_per_lateral_all_per_ft,
        {{ wv_per_meter_to_per_foot('costnormperlateralcalc') }} as cost_normalized_per_lateral_per_ft,
        {{ wv_per_meter_to_per_foot('costnormperlateralallcalc') }} as cost_normalized_per_lateral_all_per_ft,
        {{ wv_per_meter_to_per_foot('mudcostperdepthcalc') }} as mud_cost_per_depth_per_ft,
        {{ wv_per_meter_to_per_foot('mudcostperdepthnormcalc') }} as mud_cost_per_depth_normalized_per_ft,

        -- cost totals
        costfinalactual::float as cost_final_actual,
        costtotalcalc::float as total_field_estimate,
        costmaxtotalcalc::float as cost_max_total,
        costmintotalcalc::float as cost_min_total,
        costmltotalcalc::float as cost_ml_total,
        costmltotalnoplanchangecalc::float as cost_ml_total_no_plan_change,
        costmlnoexcludecalc::float as cost_ml_no_exclude,
        costtechlimittotalcalc::float as cost_tech_limit_total,
        costnormtotalcalc::float as cost_total_normalized,
        costforecastcalc::float as cost_forecast,
        costnormforecastcalc::float as cost_forecast_normalized,

        -- cost variances
        costafeforecastvarcalc::float as cost_afe_forecast_variance,
        costforecastfieldvarcalc::float as cost_forecast_field_variance,
        costnormafeforecastvarcalc::float as cost_normalized_afe_forecast_variance,
        costnormforecastfieldvarcalc::float as cost_normalized_forecast_field_variance,
        varianceafefinalcalc::float as variance_afe_final,
        variancefieldcalc::float as variance_field,
        variancefieldfinalcalc::float as variance_field_final,
        variancefinalcalc::float as variance_final,
        variancenormafefinalcalc::float as variance_normalized_afe_final,
        variancenormfieldcalc::float as variance_normalized_field,
        variancenormfieldfinalcalc::float as variance_normalized_field_final,
        variancenormfinalcalc::float as variance_normalized_final,
        estcostsavecalc::float as estimated_cost_savings,
        estcostnormsavecalc::float as estimated_cost_savings_normalized,
        estproblemcostcalc::float as estimated_problem_cost,
        estproblemcostnormcalc::float as estimated_problem_cost_normalized,

        -- mud and supply costs
        mudcostcalc::float as mud_cost,
        mudcostnormcalc::float as mud_cost_normalized,
        jobsupplycostcalc::float as job_supply_cost,
        jobsupplycostnormcalc::float as job_supply_cost_normalized,
        finalinvoicetotalcalc::float as final_invoice_total,
        finalinvoicetotalnormcalc::float as final_invoice_total_normalized,

        -- currency
        trim(currencycode)::varchar as currency_code,
        currencyexchangerate::float as currency_exchange_rate,

        -- mud properties (kg/m3 → lb/gal)
        {{ wv_kgm3_to_lb_per_gal('muddensitymaxcalc') }} as mud_density_max_ppg,
        {{ wv_kgm3_to_lb_per_gal('muddensitymincalc') }} as mud_density_min_ppg,
        {{ wv_kgm3_to_lb_per_gal('programmuddensitymaxcalc') }} as program_mud_density_max_ppg,
        {{ wv_kgm3_to_lb_per_gal('programmuddensitymincalc') }} as program_mud_density_min_ppg,
        trim(mudtypcalc)::varchar as mud_type,

        -- production rates (metric → oilfield)
        {{ wv_cbm_per_day_to_bbl_per_day('ratetargetoil') }} as target_oil_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('rateactualoil') }} as actual_oil_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratetargetwater') }} as target_water_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('rateactualwater') }} as actual_water_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('ratetargetcond') }} as target_condensate_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_bbl_per_day('rateactualcond') }} as actual_condensate_rate_bbl_per_day,
        {{ wv_cbm_per_day_to_mcf_per_day('ratetargetgas') }} as target_gas_rate_mcf_per_day,
        {{ wv_cbm_per_day_to_mcf_per_day('rateactualgas') }} as actual_gas_rate_mcf_per_day,

        -- percentages (proportion → pct)
        pctproblemtimecalc / 0.01 as problem_time_pct,
        percenttmrotatingcalc / 0.01 as rotating_time_pct,
        percenttmslidingcalc / 0.01 as sliding_time_pct,
        percentdepthrotatingcalc / 0.01 as depth_rotating_pct,
        percentdepthslidingcalc / 0.01 as depth_sliding_pct,
        ratiodepthactualplancalc / 0.01 as ratio_depth_actual_plan_pct,
        ratiodepthactualtargetcalc / 0.01 as ratio_depth_actual_target_pct,

        -- performance metrics
        bhadrillruncalc::float as bha_drill_runs,
        bhatotalruncalc::float as bha_total_runs,
        bitrevscalc::float as bit_revolutions,
        reportnocalc::float as report_count,
        ratiodurtimelogrefhourscalc::float as ratio_duration_time_log_ref_hours,

        -- safety metrics
        safetyincnocalc::float as safety_incident_count,
        safetyincreportnocalc::float as safety_reportable_incident_count,
        hazardidnorptcalc::float as hazard_id_report_count,

        -- status and results
        trim(status1)::varchar as status_primary,
        trim(status2)::varchar as status_secondary,
        trim(resulttechnical)::varchar as technical_result,
        trim(summary)::varchar as job_summary,
        trim(summarygeo)::varchar as geological_summary,

        -- responsible parties
        trim(client)::varchar as client_operator,
        trim(responsiblegrp1)::varchar as responsible_group_1,
        trim(responsiblegrp2)::varchar as responsible_group_2,
        trim(responsiblegrp3)::varchar as responsible_group_3,

        -- reference identifiers
        trim(projectrefnumbercalc)::varchar as project_reference_number,
        trim(chartofaccounts)::varchar as chart_of_accounts,
        trim(jobida)::varchar as job_id_a,
        trim(jobidb)::varchar as job_id_b,
        trim(jobidc)::varchar as job_id_c,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idrectub)::varchar as tubing_string_id,
        trim(idrectubtk)::varchar as tubing_string_table_key,
        trim(idrecwellborecalc)::varchar as calculated_wellbore_id,
        trim(idrecwellborecalctk)::varchar as calculated_wellbore_table_key,

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
        coalesce(userboolean1 = 1, false) as user_boolean_1,
        coalesce(userboolean2 = 1, false) as user_boolean_2,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and job_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['job_id']) }} as job_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        job_sk,

        -- identifiers
        job_id,
        well_id,
        wellbore_id,
        wellbore_table_key,

        -- job classification
        job_category,
        job_type_primary,
        job_type_secondary,
        complexity_index,

        -- key dates
        job_start_at,
        job_end_at,
        spud_at,
        planned_start_at,
        calculated_end_at,
        total_depth_reached_at,

        -- planned dates
        planned_end_ml_at,
        planned_end_min_at,
        planned_end_max_at,
        planned_end_tech_limit_at,

        -- objectives and targets
        job_objective,
        geological_objective,
        target_formation,

        -- depths (ft)
        target_depth_ft,
        target_depth_tvd_ft,
        total_depth_ft,
        total_depth_tvd_ft,
        depth_drilled_ft,
        depth_rotating_ft,
        depth_sliding_ft,
        planned_max_depth_ft,
        depth_drilled_per_bha_ft,
        depth_drilled_per_report_ft,
        depth_per_ratio_duration_ft,
        td_to_mud_line_ft,

        -- drilling rates (ft/hr)
        rop_ft_per_hr,
        rop_avg_from_spud_ft_per_hr,
        rop_rotating_ft_per_hr,
        rop_sliding_ft_per_hr,
        rop_spud_time_log_ft_per_hr,
        rop_time_log_ft_per_hr,
        depth_drilled_spud_to_rr_ft_per_hr,
        planned_ml_rate_ft_per_hr,

        -- time durations — days
        duration_ilt_days,
        duration_max_total_days,
        duration_min_total_days,
        duration_ml_total_days,
        duration_start_to_end_days,
        duration_tech_limit_total_days,
        duration_ml_no_plan_change_days,
        duration_ml_no_exclude_days,

        -- time durations — hours
        drilling_time_hours,
        rotating_time_hours,
        sliding_time_hours,
        circulating_time_hours,
        tripping_time_hours,
        other_time_hours,
        time_log_total_hours,
        spud_time_log_hours,
        spud_to_td_hours,
        spud_to_rr_hours,
        no_problem_time_hours,
        problem_time_hours,
        personnel_regular_hours,
        personnel_ot_hours,
        personnel_total_hours,
        spud_to_plan_ml_hours,
        spud_to_plan_min_hours,
        spud_to_plan_max_hours,
        spud_to_plan_tech_limit_hours,
        estimated_problem_time_hours,
        estimated_time_savings_hours,

        -- time durations — minutes
        duration_off_bottom_minutes,
        duration_on_bottom_minutes,
        duration_pipe_moving_minutes,

        -- afe information
        afe_number,
        afe_supplemental_number,
        afe_amount,
        afe_amount_normalized,
        afe_supplemental_amount,
        afe_supplemental_amount_normalized,
        afe_total_amount,
        afe_total_amount_normalized,
        afe_cost_type,

        -- cost per hour
        afe_per_hour,
        afe_per_hour_normalized,
        cost_per_hour,
        cost_per_hour_normalized,

        -- cost per foot
        afe_per_target_depth_per_ft,
        afe_per_target_depth_normalized_per_ft,
        cost_per_depth_per_ft,
        cost_normalized_per_depth_per_ft,
        cost_per_depth_plan_ml_per_ft,
        cost_normalized_per_depth_plan_ml_per_ft,
        cost_per_lateral_per_ft,
        cost_per_lateral_all_per_ft,
        cost_normalized_per_lateral_per_ft,
        cost_normalized_per_lateral_all_per_ft,
        mud_cost_per_depth_per_ft,
        mud_cost_per_depth_normalized_per_ft,

        -- cost totals
        cost_final_actual,
        total_field_estimate,
        cost_max_total,
        cost_min_total,
        cost_ml_total,
        cost_ml_total_no_plan_change,
        cost_ml_no_exclude,
        cost_tech_limit_total,
        cost_total_normalized,
        cost_forecast,
        cost_forecast_normalized,

        -- cost variances
        cost_afe_forecast_variance,
        cost_forecast_field_variance,
        cost_normalized_afe_forecast_variance,
        cost_normalized_forecast_field_variance,
        variance_afe_final,
        variance_field,
        variance_field_final,
        variance_final,
        variance_normalized_afe_final,
        variance_normalized_field,
        variance_normalized_field_final,
        variance_normalized_final,
        estimated_cost_savings,
        estimated_cost_savings_normalized,
        estimated_problem_cost,
        estimated_problem_cost_normalized,

        -- mud and supply costs
        mud_cost,
        mud_cost_normalized,
        job_supply_cost,
        job_supply_cost_normalized,
        final_invoice_total,
        final_invoice_total_normalized,

        -- currency
        currency_code,
        currency_exchange_rate,

        -- mud properties (ppg)
        mud_density_max_ppg,
        mud_density_min_ppg,
        program_mud_density_max_ppg,
        program_mud_density_min_ppg,
        mud_type,

        -- production rates
        target_oil_rate_bbl_per_day,
        actual_oil_rate_bbl_per_day,
        target_water_rate_bbl_per_day,
        actual_water_rate_bbl_per_day,
        target_condensate_rate_bbl_per_day,
        actual_condensate_rate_bbl_per_day,
        target_gas_rate_mcf_per_day,
        actual_gas_rate_mcf_per_day,

        -- percentages
        problem_time_pct,
        rotating_time_pct,
        sliding_time_pct,
        depth_rotating_pct,
        depth_sliding_pct,
        ratio_depth_actual_plan_pct,
        ratio_depth_actual_target_pct,

        -- performance metrics
        bha_drill_runs,
        bha_total_runs,
        bit_revolutions,
        report_count,
        ratio_duration_time_log_ref_hours,

        -- safety metrics
        safety_incident_count,
        safety_reportable_incident_count,
        hazard_id_report_count,

        -- status and results
        status_primary,
        status_secondary,
        technical_result,
        job_summary,
        geological_summary,

        -- responsible parties
        client_operator,
        responsible_group_1,
        responsible_group_2,
        responsible_group_3,

        -- reference identifiers
        project_reference_number,
        chart_of_accounts,
        job_id_a,
        job_id_b,
        job_id_c,
        last_rig_id,
        last_rig_table_key,
        tubing_string_id,
        tubing_string_table_key,
        calculated_wellbore_id,
        calculated_wellbore_table_key,

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
        created_at,
        created_by,
        last_mod_at,
        last_mod_by,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
