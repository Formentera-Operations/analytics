{{ config(
    materialized='view',
    tags=['wellview', 'stimulation', 'fracturing', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrec as record_id,

        -- Basic stimulation information
        proposedoractual as proposed_or_actual,
        typ1 as stimulation_type,
        typ2 as stimulation_subtype,
        iconname as icon_name,
        category as category,
        categorywellsno as number_of_wells,

        -- Service providers
        contractor as stim_treat_company,
        contractsupt as stim_treat_supervisor,
        diversioncontractor as diversion_company,
        diversionmethod as diversion_method,

        -- Job timing
        dttmstart as start_date,
        dttmend as end_date,
        dttmstartmincalc as min_start_date,
        dttmendmaxcalc as max_end_date,
        idrecjob as job_id,
        idrecjobtk as job_table_key,

        -- Job reference
        intervalnodesign as design_number_of_treatment_intervals,
        intervalno as actual_total_stages,

        -- Stage and interval information
        intervalnocalc as calculated_stages,
        clustersumcalc as total_number_of_clusters,
        clustersperintnocalc as clusters_per_stage,
        otherinholesumcalc as total_number_of_plugs,
        intervalnoperdurnetcalc as stages_per_day,
        otherinholeperdurnetcalc as plugs_per_day,

        -- Depths (converted to US units)
        costtotalcalc as total_cost,
        costperclustercalc as cost_per_cluster,
        costperintnocalc as cost_per_stage,
        resulttechnical as technical_result,

        -- Pump durations (converted to appropriate units)
        resulttechnicaldetail as tech_result_details,
        resulttechnicalnote as tech_result_note,
        usernum1 as user_number_1,

        -- Treatment rates (converted to BBL/MIN)
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        usertxt1 as user_text_1,
        com as comment,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,

        -- Pressures (converted to PSI)
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- Pressure gradients (converted to PSI/FT)
        systag as system_tag,
        _fivetran_synced as fivetran_synced_at,

        -- Volumes (converted to barrels)
        durationgrosscalc / 0.0416666666666667 as total_duration_gross_hours,
        durationnetcalc / 0.0416666666666667 as total_duration_net_hours,
        depthtopmincalc / 0.3048 as min_top_depth_ft,
        depthbtmmaxcalc / 0.3048 as max_bottom_depth_ft,
        lengthcalc / 0.3048 as length_gross_ft,
        lengthsumcalc / 0.3048 as length_net_ft,
        durpump / 0.000694444444444444 as total_pump_duration_minutes,
        durpumpcalc / 0.0416666666666667 as total_pump_duration_calc_hours,
        durpumpperdurnetcalc / 0.0416666666666667 as total_pumping_hours_per_day,
        ratetreatavg / 228.941712 as treat_rate_avg_bbl_per_min,
        ratetreatmax / 228.941712 as treat_rate_max_bbl_per_min,
        ratecleanavgcalc / 228.941712 as clean_rate_avg_bbl_per_min,
        ratecleanmaxcalc / 228.941712 as clean_rate_max_bbl_per_min,

        -- Proppant (converted to pounds)
        rateslurryavgcalc / 228.941712 as slurry_rate_avg_bbl_per_min,
        rateslurrymaxcalc / 228.941712 as slurry_rate_max_bbl_per_min,
        prestreatavg / 6.894757 as treat_pressure_avg_psi,
        prestreatmax / 6.894757 as treat_pressure_max_psi,
        prestreatavgcalc / 6.894757 as treat_pressure_avg_all_stages_psi,
        prestreatmaxcalc / 6.894757 as treat_pressure_max_all_stages_psi,
        presbreakdownavgcalc / 6.894757 as breakdown_pressure_avg_psi,

        -- Proppant concentrations (converted to LB/GAL)
        presclosureavgcalc / 6.894757 as closure_pressure_avg_psi,
        presscreenoutavgcalc / 6.894757 as screen_out_pressure_avg_psi,
        closuregradientavgcalc / 22.620593832021 as closure_gradient_avg_psi_per_ft,
        fracgradientavgcalc / 22.620593832021 as frac_gradient_avg_psi_per_ft,
        volcleantotal / 0.158987294928 as volume_clean_total_bbl,
        volcleantotalcalc / 0.158987294928 as volume_clean_total_calc_bbl,
        volslurrytotal / 0.158987294928 as volume_slurry_total_bbl,

        -- Performance metrics
        volslurrytotalcalc / 0.158987294928 as volume_slurry_total_calc_bbl,
        volrecoveredtotal / 0.158987294928 as volume_recovered_total_bbl,
        volrecoveredtotalcalc / 0.158987294928 as volume_recovered_total_calc_bbl,
        volnetcleancalc / 0.158987294928 as total_clean_minus_recovered_volume_bbl,

        -- Performance rates (converted to appropriate units)
        volnetslurrycalc / 0.158987294928 as total_slurry_minus_recovered_volume_bbl,
        volco2total / 0.158987294928 as volume_co2_total_bbl,
        volco2totalcalc / 0.158987294928 as volume_co2_total_calc_bbl,
        voln2total / 0.158987294928 as volume_n2_total_bbl,

        -- Cost calculations
        voln2totalcalc / 0.158987294928 as volume_n2_total_calc_bbl,
        volcleantotalperlengthcalc / 0.52161187664042 as volume_per_length_gross_bbl_per_ft,
        massproptotal / 0.45359237 as proppant_total_lb,
        massproptotalcalc / 0.45359237 as proppant_total_calc_lb,
        massproptotalperdurnetcalc / 0.45359237 as mass_proppant_per_day_lb,
        massproptotalperintnocalc / 0.45359237 as mass_proppant_per_stage_lb,

        -- Technical results
        massproptotalperintperdurcalc / 0.45359237 as mass_proppant_per_stage_per_day_lb,
        massproptotalperlengthcalc / 1.48816394356955 as mass_proppant_per_gross_length_lb_per_ft,
        massproptotalperlengthsumcalc / 1.48816394356955 as mass_proppant_per_net_length_lb_per_ft,

        -- User fields
        concbhavgcalc / 119.826428404623 as bh_conc_avg_all_stages_lb_per_gal,
        concbhmaxcalc / 119.826428404623 as bh_conc_max_all_stages_lb_per_gal,
        concbhmincalc / 119.826428404623 as bh_conc_min_all_stages_lb_per_gal,
        concsurfavgcalc / 119.826428404623 as surf_conc_avg_all_stages_lb_per_gal,

        -- Comments
        concsurfmaxcalc / 119.826428404623 as surf_conc_max_all_stages_lb_per_gal,

        -- System locking fields
        concsurfmincalc / 119.826428404623 as surf_conc_min_all_stages_lb_per_gal,
        concintavgcalc / 119.826428404623 as conc_at_zone_avg_all_stages_lb_per_gal,
        fluidefficiency / 0.01 as fluid_efficiency_percent,
        powertotal / 745.6999 as pump_power_total_hp,
        temptreat / 0.555555555555556 + 32 as treat_temperature_fahrenheit,

        -- System tracking fields
        temptreatavgcalc / 0.555555555555556 + 32 as treat_temperature_avg_fahrenheit,
        lengthperdurnetcalc / 7.3152 as length_gross_per_hour_ft_per_hr,
        lengthsumperdurnetcalc / 7.3152 as length_net_per_hour_ft_per_hr,
        costperlengthcalc / 3.28083989501312 as cost_per_length_gross_per_ft,
        costperlengthsumcalc / 3.28083989501312 as cost_per_length_net_per_ft,

        -- Fivetran metadata
        costpermassproptotalcalc / 2.20462262184878 as cost_per_mass_proppant_per_lb

    from source_data
)

select * from renamed
