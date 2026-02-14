{{ config(
    materialized='view',
    tags=['wellview', 'drilling', 'drillstring', 'parameters', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBDRILLSTRINGDRILLPARAM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as drilling_parameter_id,
        idrecparent as drill_string_id,
        idwell as well_id,

        -- Date and time information
        dttmstart as start_datetime,
        dttmend as end_datetime,
        idrecwellbore as wellbore_id,

        -- Depth information (converted to US units - feet)
        idrecwellboretk as wellbore_table_key,
        idrecjobprogramphasecalc as job_program_phase_id,
        idrecjobprogramphasecalctk as job_program_phase_table_key,
        idrecotherinholecalc as other_in_hole_id,
        idrecotherinholecalctk as other_in_hole_table_key,
        rpmstring as string_rpm,

        -- Depth drilled calculations (converted to US units - feet)
        rpmmotor as motor_rpm,
        rpmtotalcalc as total_rpm,
        rpmtotalnoexccalc as total_rpm_no_exclusions,
        bitrevscalc as bit_revolutions,
        bitrevsnoexccalc as bit_revolutions_no_exclusions,

        -- Wellbore and operational references
        bitrevscumcalc as cumulative_bit_revolutions,
        bitrevscumnoexccalc as cumulative_bit_revolutions_no_exclusions,
        inclstartcalc as start_inclination_deg,
        inclendcalc as end_inclination_deg,
        inclmaxcalc as max_inclination_deg,
        tfo as tool_face_orientation_deg,

        -- Time breakdown (converted to US units - hours)
        tforef as tool_face_orientation_reference,
        torquedrill as drilling_torque,
        torqueoffbtm as off_bottom_torque,
        torqueunits as torque_units,
        typ1 as parameter_type,
        typ2 as parameter_subtype,
        excludefromnewhole as exclude_from_new_hole,
        refnostand as stand_reference_number,
        note as notes,
        formationcalc as formation,
        outofrangecalc as out_of_range_parameters,

        -- Drilling parameters
        plugdebrisdes as plug_debris_description,
        pluggrindquality as plug_grind_quality,
        reportnocalc as report_number,
        reportdaycalc as report_day,
        daysfromspudcalc as days_from_spud,

        -- Pressures (converted to US units - PSI)
        durationnoprobtimecumcalc as cumulative_no_problem_time_days,
        durationproblemtimecumcalc as cumulative_problem_time_days,
        durationtimelogcumspudcalc as cumulative_time_log_from_spud_days,
        durationtimelogtotcumcalc as cumulative_time_log_total_days,

        -- Flow rates (converted to US units - GPM)
        rigcrewnamecalc as rig_crew_name,
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- Temperatures (converted to US units - Fahrenheit)
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,

        -- Hook loads and torque & drag (converted to US units - klbf)
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        _fivetran_synced as fivetran_synced_at,
        durationcalc / 0.000694444444444444 as duration_minutes,
        depthstart / 0.3048 as start_depth_ft,

        -- Rate of penetration (converted to US units)
        depthend / 0.3048 as end_depth_ft,
        depthstartnoexccalc / 0.3048 as start_depth_no_exclusions_ft,
        depthendnoexccalc / 0.3048 as end_depth_no_exclusions_ft,
        depthtvdstartcalc / 0.3048 as start_depth_tvd_ft,
        -- ROP instantaneous: converting from m/day to min/ft (reciprocal conversion)
        depthtvdendcalc / 0.3048 as end_depth_tvd_ft,

        -- Bit revolutions
        depthdrilledcalc / 0.3048 as depth_drilled_ft,
        depthdrillednoexccalc / 0.3048 as depth_drilled_no_exclusions_ft,
        depthdrilledcumcalc / 0.3048 as cumulative_depth_drilled_ft,
        depthdrilledcumnoexccalc / 0.3048 as cumulative_depth_drilled_no_exclusions_ft,

        -- Inclination (no conversion needed - degrees)
        lengthtoplugdepthtopcalc / 0.3048 as length_to_plug_depth_top_ft,
        tmdrill / 0.0416666666666667 as drilling_time_hr,
        tmcirc / 0.0416666666666667 as circulating_time_hr,

        -- Directional drilling parameters
        tmtrip / 0.0416666666666667 as trip_time_hr,
        tmother / 0.0416666666666667 as other_time_hr,
        tmdrillcumcalc / 0.0416666666666667 as cumulative_drilling_time_hr,

        -- Torque
        tmcirccumcalc / 0.0416666666666667 as cumulative_circulating_time_hr,
        tmtripcumcalc / 0.0416666666666667 as cumulative_trip_time_hr,
        tmothercumcalc / 0.0416666666666667 as cumulative_other_time_hr,

        -- ECD (converted to US units - lb/gal)
        tmdrillcumnoexccalc / 0.0416666666666667 as cumulative_drilling_time_no_exclusions_hr,

        -- Air/gas drilling parameters
        tmrotatingcalc / 0.0416666666666667 as rotating_time_hr,

        -- Sweep parameters
        tmslidingcalc / 0.0416666666666667 as sliding_time_hr,
        wob / 4448.2216152605 as weight_on_bit_klbf,
        sppdrill / 6.894757 as standpipe_pressure_psi,

        -- Parameter classification
        sppdiff / 6.894757 as standpipe_differential_pressure_psi,
        bhaanpres / 6.894757 as bha_annulus_pressure_psi,
        surfannpres / 6.894757 as surface_annulus_pressure_psi,

        -- Operational information
        liquidinjrate / 5.45099328 as liquid_injection_rate_gpm,
        liquidinjrateriser / 5.45099328 as liquid_injection_rate_riser_gpm,
        liquidreturnrate / 5.45099328 as liquid_return_rate_gpm,
        gasinjrate / 40.776264 as gas_injection_rate_cfm,

        -- Plug drill out information
        gasreturnrate / 40.776264 as gas_return_rate_cfm,
        bhtemp / 0.555555555555556 + 32 as bottom_hole_temperature_deg_f,
        injtemp / 0.555555555555556 + 32 as injection_temperature_deg_f,

        -- Reporting and time calculations
        surfanntemp / 0.555555555555556 + 32 as surface_annulus_temperature_deg_f,
        hookloadrotating / 4448.2216152605 as hook_load_rotating_klbf,
        hookloadpickup / 4448.2216152605 as hook_load_pickup_klbf,
        hookloadslackoff / 4448.2216152605 as hook_load_slackoff_klbf,
        hookloadoffbottom / 4448.2216152605 as hook_load_off_bottom_klbf,
        dragdowncalc / 4448.2216152605 as drag_down_klbf,
        dragupcalc / 4448.2216152605 as drag_up_klbf,

        -- Crew information
        ropcalc / 7.3152 as rop_ft_per_hr,

        -- System fields
        ropnoexccalc / 7.3152 as rop_no_exclusions_ft_per_hr,
        ropcumcalc / 7.3152 as cumulative_rop_ft_per_hr,
        ropcumnoexccalc / 7.3152 as cumulative_rop_no_exclusions_ft_per_hr,
        case
            when ropinst = 0 then null
            else power(ropinst, -1) / 0.00227836103820356
        end as rop_instantaneous_min_per_ft,
        szodvgstab / 0.0254 as vg_stabilizer_od_in,
        ecdendoverride / 119.826428404623 as ecd_end_override_lb_per_gal,
        airpercent / 0.01 as air_percent,
        sweepvol / 0.158987294928 as sweep_volume_bbl,
        sweepviscin / 0.001 as sweep_viscosity_in_cp,
        sweepviscout / 0.001 as sweep_viscosity_out_cp,

        -- Fivetran fields
        durplugdrilloutcalc / 0.000694444444444444 as plug_drill_out_duration_minutes

    from source_data
)

select * from renamed
