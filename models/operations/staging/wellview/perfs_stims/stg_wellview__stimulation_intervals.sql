{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per stimulation interval)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as parent_record_id,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idreczoneor)::varchar as zone_id,
        trim(idreczoneortk)::varchar as zone_table_key,
        trim(idreczonecalc)::varchar as linked_zone_id,
        trim(idreczonecalctk)::varchar as linked_zone_table_key,
        trim(idrecstring)::varchar as string_deployment_method_id,
        trim(idrecstringtk)::varchar as string_deployment_method_table_key,
        trim(idrecjobprogramphasecalc)::varchar as phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as phase_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idrecotherinholecalc)::varchar as other_in_hole_id,
        trim(idrecotherinholecalctk)::varchar as other_in_hole_table_key,

        -- descriptive fields
        intrefno::float as stage_number,
        trim(typ1)::varchar as stage_type,
        trim(typ2)::varchar as stage_subtype,
        trim(des)::varchar as interval_description,
        trim(formationcalc)::varchar as formation,
        trim(reservoircalc)::varchar as reservoir,
        trim(deliverymode)::varchar as delivery_mode,
        trim(presbhmethod)::varchar as bh_pressure_method,
        trim(presclosuremethod)::varchar as closure_pressure_method,
        trim(fracdiagnosticmethod)::varchar as diagnostic_method,
        trim(resulttechnical)::varchar as technical_result,
        trim(resulttechnicaldetail)::varchar as tech_result_details,
        trim(resulttechnicalnote)::varchar as tech_result_note,
        trim(usertxt1)::varchar as user_text_1,
        trim(com)::varchar as comment,

        -- equipment and perf counts
        ballsno::float as number_of_balls,
        exclude::boolean as exclude_from_calculations,
        pumpsonlineno::float as number_of_pumps_on_line,
        pumpsonlineendno::float as number_of_pumps_on_line_at_end,
        pumpsonlinenodifcalc::float as number_of_pumps_down,
        perfsopennoest::float as estimated_number_of_open_perfs,
        perfsopennocalc::float as number_of_open_perfs,
        perfshotsaltcalc::float as shot_total_alt,
        perfshotsperopencalc::float as shots_total_per_open_perfs,

        -- user numeric fields
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,

        -- duration (converted from days to hours/minutes)
        {{ wv_days_to_hours('durationcalc') }} as interval_duration_hours,
        {{ wv_days_to_hours('shutintmfinal') }} as shut_in_time_final_hours,
        {{ wv_days_to_minutes('durpump') }} as pumping_duration_minutes,
        {{ wv_days_to_minutes('durclosure') }} as closure_duration_minutes,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('lengthcalc') }} as interval_length_ft,
        {{ wv_meters_to_feet('lengthspacingcalc') }} as spacing_ft,
        {{ wv_meters_to_feet('depthtopelement') }} as top_depth_of_element_ft,
        {{ wv_meters_to_feet('depthbtmelement') }} as bottom_depth_of_element_ft,
        {{ wv_meters_to_feet('depthtopproppant') }} as estimated_sand_top_depth_ft,
        {{ wv_meters_to_feet('netpayest') }} as estimated_net_pay_ft,
        {{ wv_meters_to_feet('szballs') }} as perf_ball_size_ft,

        -- frac geometry (converted from metric to US units)
        {{ wv_meters_to_feet('fracheight') }} as frac_height_ft,
        {{ wv_meters_to_feet('fraclength') }} as frac_length_ft,
        {{ wv_meters_to_feet('fracwidth') }} as frac_width_ft,

        -- shut-in pressures (converted to PSI)
        {{ wv_kpa_to_psi('shutinpresinitial') }} as opening_wellhead_pressure_psi,
        {{ wv_kpa_to_psi('shutinpresinst') }} as instant_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres1') }} as one_minute_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres3') }} as three_minute_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres4') }} as four_minute_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres5') }} as five_minute_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres10') }} as ten_minute_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres15') }} as fifteen_minute_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpresfinal') }} as post_treatment_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpresbhfinal') }} as bh_post_treatment_shut_in_pressure_psi,
        {{ wv_kpa_to_psi('shutinpres1to4calc') }} as three_minute_bleed_off_psi,

        -- treatment pressures (converted to PSI)
        {{ wv_kpa_to_psi('presbreakdown') }} as breakdown_pressure_psi,
        {{ wv_kpa_to_psi('presbhbreakdown') }} as bh_breakdown_pressure_psi,
        {{ wv_kpa_to_psi('presscreenout') }} as screen_out_pressure_psi,
        {{ wv_kpa_to_psi('presclosure') }} as closure_pressure_psi,
        {{ wv_kpa_to_psi('presbhclosure') }} as bh_closure_pressure_psi,
        {{ wv_kpa_to_psi('preshyd') }} as hydrostatic_pressure_psi,
        {{ wv_kpa_to_psi('prestreatavg') }} as treat_pressure_avg_psi,
        {{ wv_kpa_to_psi('prestreatmin') }} as treat_pressure_min_psi,
        {{ wv_kpa_to_psi('prestreatmax') }} as treat_pressure_max_psi,
        {{ wv_kpa_to_psi('presmaxmintreatcalc') }} as treat_pressure_max_minus_min_psi,
        {{ wv_kpa_to_psi('presfrictionloss') }} as friction_pressure_loss_psi,
        {{ wv_kpa_to_psi('pressleeveshift') }} as sleeve_shift_pressure_psi,

        -- tubing pressures (converted to PSI)
        {{ wv_kpa_to_psi('presavgtubing') }} as tubing_pressure_avg_psi,
        {{ wv_kpa_to_psi('presmintubing') }} as tubing_pressure_min_psi,
        {{ wv_kpa_to_psi('presmaxtubing') }} as tubing_pressure_max_psi,
        {{ wv_kpa_to_psi('presmaxmintubingcalc') }} as tubing_pressure_max_minus_min_psi,

        -- casing pressures (converted to PSI)
        {{ wv_kpa_to_psi('presavgcasing') }} as casing_pressure_avg_psi,
        {{ wv_kpa_to_psi('presmincasing') }} as casing_pressure_min_psi,
        {{ wv_kpa_to_psi('presmaxcasing') }} as casing_pressure_max_psi,
        {{ wv_kpa_to_psi('presmaxmincasingcalc') }} as casing_pressure_max_minus_min_psi,

        -- annulus pressures (converted to PSI)
        {{ wv_kpa_to_psi('presavgannulus') }} as annulus_pressure_avg_psi,
        {{ wv_kpa_to_psi('presminannulus') }} as annulus_pressure_min_psi,
        {{ wv_kpa_to_psi('presmaxannulus') }} as annulus_pressure_max_psi,
        {{ wv_kpa_to_psi('presmaxminannuluscalc') }} as annulus_pressure_max_minus_min_psi,

        -- pressure gradients (psi/ft — inline, no macro)
        breakdowngradient / 22.620593832021 as breakdown_gradient_psi_per_ft,
        closuregradient / 22.620593832021 as closure_gradient_psi_per_ft,
        fracgradient / 22.620593832021 as frac_gradient_psi_per_ft,
        fracgradientend / 22.620593832021 as frac_gradient_post_treat_psi_per_ft,

        -- treatment rates (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanavg') }} as clean_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanmax') }} as clean_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanmin') }} as clean_rate_min_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurryavg') }} as slurry_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurrymax') }} as slurry_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurrymin') }} as slurry_rate_min_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateavgannulus') }} as annulus_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratemaxannulus') }} as annulus_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateavgcasing') }} as casing_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratemaxcasing') }} as casing_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateavgtubing') }} as tubing_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratemaxtubing') }} as tubing_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratebhavg') }} as bh_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratebhmax') }} as bh_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratebhmin') }} as bh_rate_min_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratebreakdown') }} as breakdown_rate_bbl_per_min,

        -- volumes (converted to barrels)
        {{ wv_cbm_to_bbl('volcleantotal') }} as volume_clean_total_bbl,
        {{ wv_cbm_to_bbl('volcleantotalcalc') }} as volume_clean_total_calc_bbl,
        {{ wv_cbm_to_bbl('volslurrytotal') }} as volume_slurry_total_bbl,
        {{ wv_cbm_to_bbl('volslurrytotalcalc') }} as volume_slurry_total_calc_bbl,
        {{ wv_cbm_to_bbl('volrecoveredtotal') }} as volume_recovered_total_bbl,
        {{ wv_cbm_to_bbl('volrecoveredtotalcalc') }} as volume_recovered_total_calc_bbl,
        {{ wv_cbm_to_bbl('volco2total') }} as volume_co2_total_bbl,
        {{ wv_cbm_to_bbl('volco2totalcalc') }} as volume_co2_total_calc_bbl,
        {{ wv_cbm_to_bbl('voln2total') }} as volume_n2_total_bbl,
        {{ wv_cbm_to_bbl('voln2totalcalc') }} as volume_n2_total_calc_bbl,
        {{ wv_cbm_to_bbl('volnetcleancalc') }} as total_clean_minus_recovered_volume_bbl,
        {{ wv_cbm_to_bbl('volnetslurrycalc') }} as total_slurry_minus_recovered_volume_bbl,

        -- gas volumes (converted to MCF)
        {{ wv_cbm_to_mcf('gasvollostdownhole') }} as gas_lost_downhole_mcf,
        {{ wv_cbm_to_mcf('gasvollostsurface') }} as gas_lost_surface_mcf,
        {{ wv_cbm_to_mcf('gasvollosttransport') }} as gas_lost_transport_mcf,

        -- surface concentrations (converted to LB/GAL)
        {{ wv_kgm3_to_lb_per_gal('concsurfavg') }} as surf_conc_avg_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfmax') }} as surf_conc_max_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfmin') }} as surf_conc_min_lb_per_gal,

        -- bottom hole concentrations (converted to LB/GAL)
        {{ wv_kgm3_to_lb_per_gal('concbhavg') }} as bh_conc_avg_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhmax') }} as bh_conc_max_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhmin') }} as bh_conc_min_lb_per_gal,

        -- zone concentration (converted to LB/GAL)
        {{ wv_kgm3_to_lb_per_gal('concint') }} as concentration_at_zone_lb_per_gal,

        -- proppant masses (converted to pounds)
        {{ wv_kg_to_lb('masspropdesign') }} as proppant_designed_lb,
        {{ wv_kg_to_lb('masspropinfrm') }} as proppant_in_formation_lb,
        {{ wv_kg_to_lb('masspropinwellbore') }} as proppant_in_wellbore_lb,
        {{ wv_kg_to_lb('masspropreturn') }} as proppant_return_to_surface_lb,
        {{ wv_kg_to_lb('massproptotal') }} as proppant_total_lb,
        {{ wv_kg_to_lb('massproptotalcalc') }} as proppant_total_calc_lb,

        -- proppant design ratio (inline — percent, no macro)
        ratiopropdesigntotal / 0.01 as proppant_total_design_ratio_percent,

        -- pump power (converted to HP)
        {{ wv_watts_to_hp('pumppoweravg') }} as pump_power_avg_hp,
        {{ wv_watts_to_hp('pumppowerfluid') }} as fluid_pump_power_hp,
        {{ wv_watts_to_hp('pumppowerco2') }} as co2_pump_power_hp,
        {{ wv_watts_to_hp('pumppowermax') }} as pump_power_max_hp,

        -- temperatures (converted to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempstaticavg') }} as static_temperature_fahrenheit,
        {{ wv_celsius_to_fahrenheit('temptreatavg') }} as treat_temperature_fahrenheit,

        -- fluid efficiency (inline — percent, no macro)
        fluidefficiency / 0.01 as fluid_efficiency_percent,

        -- dates
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,
        dttmstartmincalc::timestamp_ntz as min_start_date,
        dttmendmaxcalc::timestamp_ntz as max_end_date,

        -- system locking
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,

        -- ingestion metadata
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
        and record_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as stimulation_interval_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        stimulation_interval_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,
        wellbore_id,
        wellbore_table_key,
        zone_id,
        zone_table_key,
        linked_zone_id,
        linked_zone_table_key,
        string_deployment_method_id,
        string_deployment_method_table_key,
        phase_id,
        phase_table_key,
        last_rig_id,
        last_rig_table_key,
        other_in_hole_id,
        other_in_hole_table_key,

        -- descriptive fields
        stage_number,
        stage_type,
        stage_subtype,
        interval_description,
        formation,
        reservoir,
        delivery_mode,
        bh_pressure_method,
        closure_pressure_method,
        diagnostic_method,
        technical_result,
        tech_result_details,
        tech_result_note,
        user_text_1,
        comment,

        -- equipment and perf counts
        number_of_balls,
        exclude_from_calculations,
        number_of_pumps_on_line,
        number_of_pumps_on_line_at_end,
        number_of_pumps_down,
        estimated_number_of_open_perfs,
        number_of_open_perfs,
        shot_total_alt,
        shots_total_per_open_perfs,

        -- user numeric fields
        user_number_1,
        user_number_2,
        user_number_3,

        -- durations
        interval_duration_hours,
        shut_in_time_final_hours,
        pumping_duration_minutes,
        closure_duration_minutes,

        -- depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        interval_length_ft,
        spacing_ft,
        top_depth_of_element_ft,
        bottom_depth_of_element_ft,
        estimated_sand_top_depth_ft,
        estimated_net_pay_ft,
        perf_ball_size_ft,

        -- frac geometry
        frac_height_ft,
        frac_length_ft,
        frac_width_ft,

        -- shut-in pressures
        opening_wellhead_pressure_psi,
        instant_shut_in_pressure_psi,
        one_minute_shut_in_pressure_psi,
        three_minute_shut_in_pressure_psi,
        four_minute_shut_in_pressure_psi,
        five_minute_shut_in_pressure_psi,
        ten_minute_shut_in_pressure_psi,
        fifteen_minute_shut_in_pressure_psi,
        post_treatment_shut_in_pressure_psi,
        bh_post_treatment_shut_in_pressure_psi,
        three_minute_bleed_off_psi,

        -- treatment pressures
        breakdown_pressure_psi,
        bh_breakdown_pressure_psi,
        screen_out_pressure_psi,
        closure_pressure_psi,
        bh_closure_pressure_psi,
        hydrostatic_pressure_psi,
        treat_pressure_avg_psi,
        treat_pressure_min_psi,
        treat_pressure_max_psi,
        treat_pressure_max_minus_min_psi,
        friction_pressure_loss_psi,
        sleeve_shift_pressure_psi,

        -- tubing pressures
        tubing_pressure_avg_psi,
        tubing_pressure_min_psi,
        tubing_pressure_max_psi,
        tubing_pressure_max_minus_min_psi,

        -- casing pressures
        casing_pressure_avg_psi,
        casing_pressure_min_psi,
        casing_pressure_max_psi,
        casing_pressure_max_minus_min_psi,

        -- annulus pressures
        annulus_pressure_avg_psi,
        annulus_pressure_min_psi,
        annulus_pressure_max_psi,
        annulus_pressure_max_minus_min_psi,

        -- pressure gradients
        breakdown_gradient_psi_per_ft,
        closure_gradient_psi_per_ft,
        frac_gradient_psi_per_ft,
        frac_gradient_post_treat_psi_per_ft,

        -- treatment rates
        clean_rate_avg_bbl_per_min,
        clean_rate_max_bbl_per_min,
        clean_rate_min_bbl_per_min,
        slurry_rate_avg_bbl_per_min,
        slurry_rate_max_bbl_per_min,
        slurry_rate_min_bbl_per_min,
        annulus_rate_avg_bbl_per_min,
        annulus_rate_max_bbl_per_min,
        casing_rate_avg_bbl_per_min,
        casing_rate_max_bbl_per_min,
        tubing_rate_avg_bbl_per_min,
        tubing_rate_max_bbl_per_min,
        bh_rate_avg_bbl_per_min,
        bh_rate_max_bbl_per_min,
        bh_rate_min_bbl_per_min,
        breakdown_rate_bbl_per_min,

        -- volumes
        volume_clean_total_bbl,
        volume_clean_total_calc_bbl,
        volume_slurry_total_bbl,
        volume_slurry_total_calc_bbl,
        volume_recovered_total_bbl,
        volume_recovered_total_calc_bbl,
        volume_co2_total_bbl,
        volume_co2_total_calc_bbl,
        volume_n2_total_bbl,
        volume_n2_total_calc_bbl,
        total_clean_minus_recovered_volume_bbl,
        total_slurry_minus_recovered_volume_bbl,

        -- gas volumes
        gas_lost_downhole_mcf,
        gas_lost_surface_mcf,
        gas_lost_transport_mcf,

        -- surface concentrations
        surf_conc_avg_lb_per_gal,
        surf_conc_max_lb_per_gal,
        surf_conc_min_lb_per_gal,

        -- bottom hole concentrations
        bh_conc_avg_lb_per_gal,
        bh_conc_max_lb_per_gal,
        bh_conc_min_lb_per_gal,

        -- zone concentration
        concentration_at_zone_lb_per_gal,

        -- proppant masses
        proppant_designed_lb,
        proppant_in_formation_lb,
        proppant_in_wellbore_lb,
        proppant_return_to_surface_lb,
        proppant_total_lb,
        proppant_total_calc_lb,
        proppant_total_design_ratio_percent,

        -- pump power
        pump_power_avg_hp,
        fluid_pump_power_hp,
        co2_pump_power_hp,
        pump_power_max_hp,

        -- temperatures
        static_temperature_fahrenheit,
        treat_temperature_fahrenheit,

        -- fluid efficiency
        fluid_efficiency_percent,

        -- dates
        start_date,
        end_date,
        min_start_date,
        max_end_date,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
