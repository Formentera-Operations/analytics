{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLSTRINGDRILLPARAM') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as drilling_parameter_id,
        trim(idrecparent)::varchar as drill_string_id,
        trim(idwell)::varchar as well_id,

        -- wellbore and operational references
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idrecjobprogramphasecalc)::varchar as job_program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as job_program_phase_table_key,
        trim(idrecotherinholecalc)::varchar as other_in_hole_id,
        trim(idrecotherinholecalctk)::varchar as other_in_hole_table_key,

        -- date and time information
        dttmstart::timestamp_ntz as start_datetime,
        dttmend::timestamp_ntz as end_datetime,

        -- duration (converted from days to minutes)
        {{ wv_days_to_minutes('durationcalc') }} as duration_minutes,

        -- depth information (converted from meters to feet)
        {{ wv_meters_to_feet('depthstart') }} as start_depth_ft,
        {{ wv_meters_to_feet('depthend') }} as end_depth_ft,
        {{ wv_meters_to_feet('depthstartnoexccalc') }} as start_depth_no_exclusions_ft,
        {{ wv_meters_to_feet('depthendnoexccalc') }} as end_depth_no_exclusions_ft,
        {{ wv_meters_to_feet('depthtvdstartcalc') }} as start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdendcalc') }} as end_depth_tvd_ft,

        -- depth drilled (converted from meters to feet)
        {{ wv_meters_to_feet('depthdrilledcalc') }} as depth_drilled_ft,
        {{ wv_meters_to_feet('depthdrillednoexccalc') }} as depth_drilled_no_exclusions_ft,
        {{ wv_meters_to_feet('depthdrilledcumcalc') }} as cumulative_depth_drilled_ft,
        {{ wv_meters_to_feet('depthdrilledcumnoexccalc') }} as cumulative_depth_drilled_no_exclusions_ft,
        {{ wv_meters_to_feet('lengthtoplugdepthtopcalc') }} as length_to_plug_depth_top_ft,

        -- time breakdown (converted from days to hours)
        {{ wv_days_to_hours('tmdrill') }} as drilling_time_hr,
        {{ wv_days_to_hours('tmcirc') }} as circulating_time_hr,
        {{ wv_days_to_hours('tmtrip') }} as trip_time_hr,
        {{ wv_days_to_hours('tmother') }} as other_time_hr,
        {{ wv_days_to_hours('tmdrillcumcalc') }} as cumulative_drilling_time_hr,
        {{ wv_days_to_hours('tmcirccumcalc') }} as cumulative_circulating_time_hr,
        {{ wv_days_to_hours('tmtripcumcalc') }} as cumulative_trip_time_hr,
        {{ wv_days_to_hours('tmothercumcalc') }} as cumulative_other_time_hr,
        {{ wv_days_to_hours('tmdrillcumnoexccalc') }} as cumulative_drilling_time_no_exclusions_hr,
        {{ wv_days_to_hours('tmrotatingcalc') }} as rotating_time_hr,
        {{ wv_days_to_hours('tmslidingcalc') }} as sliding_time_hr,

        -- rate of penetration (converted from m/s to ft/hr)
        {{ wv_mps_to_ft_per_hr('ropcalc') }} as rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropnoexccalc') }} as rop_no_exclusions_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropcumcalc') }} as cumulative_rop_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ropcumnoexccalc') }} as cumulative_rop_no_exclusions_ft_per_hr,

        -- rop instantaneous: converting from m/day to min/ft (reciprocal conversion)
        case
            when ropinst = 0 then null
            else power(ropinst, -1) / 0.00227836103820356
        end as rop_instantaneous_min_per_ft,

        -- drilling parameters
        {{ wv_newtons_to_klbf('wob') }} as weight_on_bit_klbf,
        {{ wv_kpa_to_psi('sppdrill') }} as standpipe_pressure_psi,
        {{ wv_kpa_to_psi('sppdiff') }} as standpipe_differential_pressure_psi,
        {{ wv_kpa_to_psi('bhaanpres') }} as bha_annulus_pressure_psi,
        {{ wv_kpa_to_psi('surfannpres') }} as surface_annulus_pressure_psi,

        -- flow rates (converted from m3/s to gpm and cfm)
        {{ wv_cbm_per_sec_to_gpm('liquidinjrate') }} as liquid_injection_rate_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidinjrateriser') }} as liquid_injection_rate_riser_gpm,
        {{ wv_cbm_per_sec_to_gpm('liquidreturnrate') }} as liquid_return_rate_gpm,
        -- gas rate: m3/s -> cfm (/ 40.776264)
        gasinjrate / 40.776264 as gas_injection_rate_cfm,
        gasreturnrate / 40.776264 as gas_return_rate_cfm,

        -- temperatures (converted from celsius to fahrenheit)
        {{ wv_celsius_to_fahrenheit('bhtemp') }} as bottom_hole_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('injtemp') }} as injection_temperature_deg_f,
        {{ wv_celsius_to_fahrenheit('surfanntemp') }} as surface_annulus_temperature_deg_f,

        -- hook loads and torque & drag (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('hookloadrotating') }} as hook_load_rotating_klbf,
        {{ wv_newtons_to_klbf('hookloadpickup') }} as hook_load_pickup_klbf,
        {{ wv_newtons_to_klbf('hookloadslackoff') }} as hook_load_slackoff_klbf,
        {{ wv_newtons_to_klbf('hookloadoffbottom') }} as hook_load_off_bottom_klbf,
        {{ wv_newtons_to_klbf('dragdowncalc') }} as drag_down_klbf,
        {{ wv_newtons_to_klbf('dragupcalc') }} as drag_up_klbf,

        -- rpm parameters
        rpmstring::float as string_rpm,
        rpmmotor::float as motor_rpm,
        rpmtotalcalc::float as total_rpm,
        rpmtotalnoexccalc::float as total_rpm_no_exclusions,

        -- bit revolutions
        bitrevscalc::float as bit_revolutions,
        bitrevsnoexccalc::float as bit_revolutions_no_exclusions,
        bitrevscumcalc::float as cumulative_bit_revolutions,
        bitrevscumnoexccalc::float as cumulative_bit_revolutions_no_exclusions,

        -- inclination (degrees, no conversion needed)
        inclstartcalc::float as start_inclination_deg,
        inclendcalc::float as end_inclination_deg,
        inclmaxcalc::float as max_inclination_deg,

        -- directional drilling parameters
        tfo::float as tool_face_orientation_deg,
        trim(tforef)::varchar as tool_face_orientation_reference,

        -- torque
        torquedrill::float as drilling_torque,
        torqueoffbtm::float as off_bottom_torque,
        trim(torqueunits)::varchar as torque_units,

        -- ecd override (converted from kg/m3 to lb/gal)
        {{ wv_kgm3_to_lb_per_gal('ecdendoverride') }} as ecd_end_override_lb_per_gal,

        -- vg stabilizer (converted from meters to inches)
        {{ wv_meters_to_inches('szodvgstab') }} as vg_stabilizer_od_in,

        -- air/gas drilling parameters
        airpercent / 0.01 as air_percent,

        -- sweep parameters (converted to bbl and cp)
        {{ wv_cbm_to_bbl('sweepvol') }} as sweep_volume_bbl,
        {{ wv_pas_to_cp('sweepviscin') }} as sweep_viscosity_in_cp,
        {{ wv_pas_to_cp('sweepviscout') }} as sweep_viscosity_out_cp,

        -- parameter classification
        trim(typ1)::varchar as parameter_type,
        trim(typ2)::varchar as parameter_subtype,
        excludefromnewhole::boolean as exclude_from_new_hole,
        trim(refnostand)::varchar as stand_reference_number,

        -- operational information
        trim(note)::varchar as notes,
        trim(formationcalc)::varchar as formation,
        trim(outofrangecalc)::varchar as out_of_range_parameters,

        -- plug drill out information
        trim(plugdebrisdes)::varchar as plug_debris_description,
        trim(pluggrindquality)::varchar as plug_grind_quality,
        {{ wv_days_to_minutes('durplugdrilloutcalc') }} as plug_drill_out_duration_minutes,

        -- reporting and time calculations
        reportnocalc::int as report_number,
        reportdaycalc::int as report_day,
        daysfromspudcalc::float as days_from_spud,
        durationnoprobtimecumcalc::float as cumulative_no_problem_time_days,
        durationproblemtimecumcalc::float as cumulative_problem_time_days,
        durationtimelogcumspudcalc::float as cumulative_time_log_from_spud_days,
        durationtimelogtotcumcalc::float as cumulative_time_log_total_days,

        -- crew information
        trim(rigcrewnamecalc)::varchar as rig_crew_name,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

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
        and drilling_parameter_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['drilling_parameter_id']) }} as drilling_parameter_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        drilling_parameter_sk,

        -- identifiers
        drilling_parameter_id,
        drill_string_id,
        well_id,

        -- wellbore and operational references
        wellbore_id,
        wellbore_table_key,
        job_program_phase_id,
        job_program_phase_table_key,
        other_in_hole_id,
        other_in_hole_table_key,

        -- date and time information
        start_datetime,
        end_datetime,
        duration_minutes,

        -- depth information
        start_depth_ft,
        end_depth_ft,
        start_depth_no_exclusions_ft,
        end_depth_no_exclusions_ft,
        start_depth_tvd_ft,
        end_depth_tvd_ft,

        -- depth drilled
        depth_drilled_ft,
        depth_drilled_no_exclusions_ft,
        cumulative_depth_drilled_ft,
        cumulative_depth_drilled_no_exclusions_ft,
        length_to_plug_depth_top_ft,

        -- time breakdown
        drilling_time_hr,
        circulating_time_hr,
        trip_time_hr,
        other_time_hr,
        cumulative_drilling_time_hr,
        cumulative_circulating_time_hr,
        cumulative_trip_time_hr,
        cumulative_other_time_hr,
        cumulative_drilling_time_no_exclusions_hr,
        rotating_time_hr,
        sliding_time_hr,

        -- rate of penetration
        rop_ft_per_hr,
        rop_no_exclusions_ft_per_hr,
        cumulative_rop_ft_per_hr,
        cumulative_rop_no_exclusions_ft_per_hr,
        rop_instantaneous_min_per_ft,

        -- drilling parameters
        weight_on_bit_klbf,
        standpipe_pressure_psi,
        standpipe_differential_pressure_psi,
        bha_annulus_pressure_psi,
        surface_annulus_pressure_psi,

        -- flow rates
        liquid_injection_rate_gpm,
        liquid_injection_rate_riser_gpm,
        liquid_return_rate_gpm,
        gas_injection_rate_cfm,
        gas_return_rate_cfm,

        -- temperatures
        bottom_hole_temperature_deg_f,
        injection_temperature_deg_f,
        surface_annulus_temperature_deg_f,

        -- hook loads and torque & drag
        hook_load_rotating_klbf,
        hook_load_pickup_klbf,
        hook_load_slackoff_klbf,
        hook_load_off_bottom_klbf,
        drag_down_klbf,
        drag_up_klbf,

        -- rpm parameters
        string_rpm,
        motor_rpm,
        total_rpm,
        total_rpm_no_exclusions,

        -- bit revolutions
        bit_revolutions,
        bit_revolutions_no_exclusions,
        cumulative_bit_revolutions,
        cumulative_bit_revolutions_no_exclusions,

        -- inclination
        start_inclination_deg,
        end_inclination_deg,
        max_inclination_deg,

        -- directional drilling parameters
        tool_face_orientation_deg,
        tool_face_orientation_reference,

        -- torque
        drilling_torque,
        off_bottom_torque,
        torque_units,

        -- ecd override
        ecd_end_override_lb_per_gal,

        -- vg stabilizer
        vg_stabilizer_od_in,

        -- air/gas drilling parameters
        air_percent,

        -- sweep parameters
        sweep_volume_bbl,
        sweep_viscosity_in_cp,
        sweep_viscosity_out_cp,

        -- parameter classification
        parameter_type,
        parameter_subtype,
        exclude_from_new_hole,
        stand_reference_number,

        -- operational information
        notes,
        formation,
        out_of_range_parameters,

        -- plug drill out information
        plug_debris_description,
        plug_grind_quality,
        plug_drill_out_duration_minutes,

        -- reporting and time calculations
        report_number,
        report_day,
        days_from_spud,
        cumulative_no_problem_time_days,
        cumulative_problem_time_days,
        cumulative_time_log_from_spud_days,
        cumulative_time_log_total_days,

        -- crew information
        rig_crew_name,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
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
