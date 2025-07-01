{{ config(
    materialized='view',
    tags=['wellview', 'stimulation', 'intervals', 'stages', 'fracturing', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Stage information
        intrefno as stage_number,
        typ1 as stage_type,
        typ2 as stage_subtype,
        des as interval_description,
        
        -- Timing
        dttmstart as start_date,
        dttmend as end_date,
        dttmstartmincalc as min_start_date,
        dttmendmaxcalc as max_end_date,
        durationcalc / 0.0416666666666667 as interval_duration_hours,
        
        -- Depths (converted to US units)
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        lengthcalc / 0.3048 as interval_length_ft,
        lengthspacingcalc / 0.3048 as spacing_ft,
        depthtopelement / 0.3048 as top_depth_of_element_ft,
        depthbtmelement / 0.3048 as bottom_depth_of_element_ft,
        depthtopproppant / 0.3048 as estimated_sand_top_depth_ft,
        netpayest / 0.3048 as estimated_net_pay_ft,
        
        -- Wellbore and zone relationships
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idreczoneor as zone_id,
        idreczoneortk as zone_table_key,
        idreczonecalc as linked_zone_id,
        idreczonecalctk as linked_zone_table_key,
        formationcalc as formation,
        reservoircalc as reservoir,
        
        -- Delivery and equipment
        deliverymode as delivery_mode,
        idrecstring as string_deployment_method_id,
        idrecstringtk as string_deployment_method_table_key,
        ballsno as number_of_balls,
        szballs / 0.3048 as perf_ball_size_ft,
        
        -- Exclusion flag
        exclude as exclude_from_calculations,
        
        -- Shut-in pressures (converted to PSI)
        shutinpresinitial / 6.894757 as opening_wellhead_pressure_psi,
        shutinpresinst / 6.894757 as instant_shut_in_pressure_psi,
        shutinpres1 / 6.894757 as one_minute_shut_in_pressure_psi,
        shutinpres3 / 6.894757 as three_minute_shut_in_pressure_psi,
        shutinpres4 / 6.894757 as four_minute_shut_in_pressure_psi,
        shutinpres5 / 6.894757 as five_minute_shut_in_pressure_psi,
        shutinpres10 / 6.894757 as ten_minute_shut_in_pressure_psi,
        shutinpres15 / 6.894757 as fifteen_minute_shut_in_pressure_psi,
        shutinpresfinal / 6.894757 as post_treatment_shut_in_pressure_psi,
        shutinpresbhfinal / 6.894757 as bh_post_treatment_shut_in_pressure_psi,
        shutintmfinal / 0.0416666666666667 as shut_in_time_final_hours,
        shutinpres1to4calc / 6.894757 as three_minute_bleed_off_psi,
        
        -- Volumes (converted to barrels)
        volcleantotal / 0.158987294928 as volume_clean_total_bbl,
        volcleantotalcalc / 0.158987294928 as volume_clean_total_calc_bbl,
        volslurrytotal / 0.158987294928 as volume_slurry_total_bbl,
        volslurrytotalcalc / 0.158987294928 as volume_slurry_total_calc_bbl,
        volrecoveredtotal / 0.158987294928 as volume_recovered_total_bbl,
        volrecoveredtotalcalc / 0.158987294928 as volume_recovered_total_calc_bbl,
        volco2total / 0.158987294928 as volume_co2_total_bbl,
        volco2totalcalc / 0.158987294928 as volume_co2_total_calc_bbl,
        voln2total / 0.158987294928 as volume_n2_total_bbl,
        voln2totalcalc / 0.158987294928 as volume_n2_total_calc_bbl,
        volnetcleancalc / 0.158987294928 as total_clean_minus_recovered_volume_bbl,
        volnetslurrycalc / 0.158987294928 as total_slurry_minus_recovered_volume_bbl,
        
        -- Gas volumes (converted to MCF)
        gasvollostdownhole / 28.316846592 as gas_lost_downhole_mcf,
        gasvollostsurface / 28.316846592 as gas_lost_surface_mcf,
        gasvollosttransport / 28.316846592 as gas_lost_transport_mcf,
        
        -- Proppant mass and concentrations
        -- Surface concentrations (converted to LB/GAL)
        concsurfavg / 119.826428404623 as surf_conc_avg_lb_per_gal,
        concsurfmax / 119.826428404623 as surf_conc_max_lb_per_gal,
        concsurfmin / 119.826428404623 as surf_conc_min_lb_per_gal,
        
        -- Bottom hole concentrations (converted to LB/GAL)
        concbhavg / 119.826428404623 as bh_conc_avg_lb_per_gal,
        concbhmax / 119.826428404623 as bh_conc_max_lb_per_gal,
        concbhmin / 119.826428404623 as bh_conc_min_lb_per_gal,
        
        -- Zone concentration (converted to LB/GAL)
        concint / 119.826428404623 as concentration_at_zone_lb_per_gal,
        
        -- Proppant masses (converted to pounds)
        masspropdesign / 0.45359237 as proppant_designed_lb,
        masspropinfrm / 0.45359237 as proppant_in_formation_lb,
        masspropinwellbore / 0.45359237 as proppant_in_wellbore_lb,
        masspropreturn / 0.45359237 as proppant_return_to_surface_lb,
        massproptotal / 0.45359237 as proppant_total_lb,
        massproptotalcalc / 0.45359237 as proppant_total_calc_lb,
        ratiopropdesigntotal / 0.01 as proppant_total_design_ratio_percent,
        
        -- Pressure measurements (converted to PSI)
        presbhmethod as bh_pressure_method,
        presbreakdown / 6.894757 as breakdown_pressure_psi,
        presbhbreakdown / 6.894757 as bh_breakdown_pressure_psi,
        presscreenout / 6.894757 as screen_out_pressure_psi,
        presclosure / 6.894757 as closure_pressure_psi,
        presbhclosure / 6.894757 as bh_closure_pressure_psi,
        presclosuremethod as closure_pressure_method,
        preshyd / 6.894757 as hydrostatic_pressure_psi,
        prestreatavg / 6.894757 as treat_pressure_avg_psi,
        prestreatmin / 6.894757 as treat_pressure_min_psi,
        prestreatmax / 6.894757 as treat_pressure_max_psi,
        presmaxmintreatcalc / 6.894757 as treat_pressure_max_minus_min_psi,
        
        -- Tubing pressures (converted to PSI)
        presavgtubing / 6.894757 as tubing_pressure_avg_psi,
        presmintubing / 6.894757 as tubing_pressure_min_psi,
        presmaxtubing / 6.894757 as tubing_pressure_max_psi,
        presmaxmintubingcalc / 6.894757 as tubing_pressure_max_minus_min_psi,
        
        -- Casing pressures (converted to PSI)
        presavgcasing / 6.894757 as casing_pressure_avg_psi,
        presmincasing / 6.894757 as casing_pressure_min_psi,
        presmaxcasing / 6.894757 as casing_pressure_max_psi,
        presmaxmincasingcalc / 6.894757 as casing_pressure_max_minus_min_psi,
        
        -- Annulus pressures (converted to PSI)
        presavgannulus / 6.894757 as annulus_pressure_avg_psi,
        presminannulus / 6.894757 as annulus_pressure_min_psi,
        presmaxannulus / 6.894757 as annulus_pressure_max_psi,
        presmaxminannuluscalc / 6.894757 as annulus_pressure_max_minus_min_psi,
        
        -- Other pressures (converted to PSI)
        presfrictionloss / 6.894757 as friction_pressure_loss_psi,
        pressleeveshift / 6.894757 as sleeve_shift_pressure_psi,
        
        -- Rates (converted to BBL/MIN)
        ratecleanavg / 228.941712 as clean_rate_avg_bbl_per_min,
        ratecleanmax / 228.941712 as clean_rate_max_bbl_per_min,
        ratecleanmin / 228.941712 as clean_rate_min_bbl_per_min,
        rateslurryavg / 228.941712 as slurry_rate_avg_bbl_per_min,
        rateslurrymax / 228.941712 as slurry_rate_max_bbl_per_min,
        rateslurrymin / 228.941712 as slurry_rate_min_bbl_per_min,
        rateavgannulus / 228.941712 as annulus_rate_avg_bbl_per_min,
        ratemaxannulus / 228.941712 as annulus_rate_max_bbl_per_min,
        rateavgcasing / 228.941712 as casing_rate_avg_bbl_per_min,
        ratemaxcasing / 228.941712 as casing_rate_max_bbl_per_min,
        rateavgtubing / 228.941712 as tubing_rate_avg_bbl_per_min,
        ratemaxtubing / 228.941712 as tubing_rate_max_bbl_per_min,
        ratebhavg / 228.941712 as bh_rate_avg_bbl_per_min,
        ratebhmax / 228.941712 as bh_rate_max_bbl_per_min,
        ratebhmin / 228.941712 as bh_rate_min_bbl_per_min,
        ratebreakdown / 228.941712 as breakdown_rate_bbl_per_min,
        
        -- Pump information
        pumpsonlineno as number_of_pumps_on_line,
        pumpsonlineendno as number_of_pumps_on_line_at_end,
        pumpsonlinenodifcalc as number_of_pumps_down,
        durpump / 0.000694444444444444 as pumping_duration_minutes,
        durclosure / 0.000694444444444444 as closure_duration_minutes,
        
        -- Pump power (converted to horsepower)
        pumppoweravg / 745.6999 as pump_power_avg_hp,
        pumppowerfluid / 745.6999 as fluid_pump_power_hp,
        pumppowerco2 / 745.6999 as co2_pump_power_hp,
        pumppowermax / 745.6999 as pump_power_max_hp,
        
        -- Perforation information
        perfsopennoest as estimated_number_of_open_perfs,
        perfsopennocalc as number_of_open_perfs,
        perfshotsaltcalc as shot_total_alt,
        perfshotsperopencalc as shots_total_per_open_perfs,
        
        -- Technical results
        breakdowngradient / 22.620593832021 as breakdown_gradient_psi_per_ft,
        closuregradient / 22.620593832021 as closure_gradient_psi_per_ft,
        fracdiagnosticmethod as diagnostic_method,
        fracgradient / 22.620593832021 as frac_gradient_psi_per_ft,
        fracgradientend / 22.620593832021 as frac_gradient_post_treat_psi_per_ft,
        fracheight / 0.3048 as frac_height_ft,
        fraclength / 0.3048 as frac_length_ft,
        fracwidth / 0.3048 as frac_width_ft,
        resulttechnical as technical_result,
        resulttechnicaldetail as tech_result_details,
        resulttechnicalnote as tech_result_note,
        
        -- Temperature and efficiency
        tempstaticavg / 0.555555555555556 + 32 as static_temperature_fahrenheit,
        temptreatavg / 0.555555555555556 + 32 as treat_temperature_fahrenheit,
        fluidefficiency / 0.01 as fluid_efficiency_percent,
        
        -- Phase and equipment references
        idrecjobprogramphasecalc as phase_id,
        idrecjobprogramphasecalctk as phase_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idrecotherinholecalc as other_in_hole_id,
        idrecotherinholecalctk as other_in_hole_table_key,
        
        -- User fields
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        usertxt1 as user_text_1,
        
        -- Comments
        com as comment,

        -- System locking fields
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System tracking fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed