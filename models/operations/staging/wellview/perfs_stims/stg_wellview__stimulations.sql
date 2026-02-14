{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per stimulation record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIM') }}
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
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(typ1)::varchar as stimulation_type,
        trim(typ2)::varchar as stimulation_subtype,
        trim(iconname)::varchar as icon_name,
        trim(category)::varchar as category,
        categorywellsno::float as number_of_wells,
        trim(contractor)::varchar as stim_treat_company,
        trim(contractsupt)::varchar as stim_treat_supervisor,
        trim(diversioncontractor)::varchar as diversion_company,
        trim(diversionmethod)::varchar as diversion_method,
        trim(resulttechnical)::varchar as technical_result,
        trim(resulttechnicaldetail)::varchar as tech_result_details,
        trim(resulttechnicalnote)::varchar as tech_result_note,
        trim(usertxt1)::varchar as user_text_1,
        trim(com)::varchar as comment,

        -- stage and interval information
        intervalnodesign::float as design_number_of_treatment_intervals,
        intervalno::float as actual_total_stages,
        intervalnocalc::float as calculated_stages,
        clustersumcalc::float as total_number_of_clusters,
        clustersperintnocalc::float as clusters_per_stage,
        otherinholesumcalc::float as total_number_of_plugs,
        intervalnoperdurnetcalc::float as stages_per_day,
        otherinholeperdurnetcalc::float as plugs_per_day,

        -- cost calculations
        costtotalcalc::float as total_cost,
        costperclustercalc::float as cost_per_cluster,
        costperintnocalc::float as cost_per_stage,

        -- user numeric fields
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,

        -- durations (converted from days to hours/minutes)
        {{ wv_days_to_hours('durationgrosscalc') }} as total_duration_gross_hours,
        {{ wv_days_to_hours('durationnetcalc') }} as total_duration_net_hours,
        {{ wv_days_to_hours('durpumpcalc') }} as total_pump_duration_calc_hours,
        {{ wv_days_to_hours('durpumpperdurnetcalc') }} as total_pumping_hours_per_day,
        {{ wv_days_to_minutes('durpump') }} as total_pump_duration_minutes,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtopmincalc') }} as min_top_depth_ft,
        {{ wv_meters_to_feet('depthbtmmaxcalc') }} as max_bottom_depth_ft,
        {{ wv_meters_to_feet('lengthcalc') }} as length_gross_ft,
        {{ wv_meters_to_feet('lengthsumcalc') }} as length_net_ft,

        -- treatment rates (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('ratetreatavg') }} as treat_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratetreatmax') }} as treat_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanavgcalc') }} as clean_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanmaxcalc') }} as clean_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurryavgcalc') }} as slurry_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurrymaxcalc') }} as slurry_rate_max_bbl_per_min,

        -- pressures (converted to PSI)
        {{ wv_kpa_to_psi('prestreatavg') }} as treat_pressure_avg_psi,
        {{ wv_kpa_to_psi('prestreatmax') }} as treat_pressure_max_psi,
        {{ wv_kpa_to_psi('prestreatavgcalc') }} as treat_pressure_avg_all_stages_psi,
        {{ wv_kpa_to_psi('prestreatmaxcalc') }} as treat_pressure_max_all_stages_psi,
        {{ wv_kpa_to_psi('presbreakdownavgcalc') }} as breakdown_pressure_avg_psi,
        {{ wv_kpa_to_psi('presclosureavgcalc') }} as closure_pressure_avg_psi,
        {{ wv_kpa_to_psi('presscreenoutavgcalc') }} as screen_out_pressure_avg_psi,

        -- pressure gradients (psi/ft — inline, no macro)
        closuregradientavgcalc / 22.620593832021 as closure_gradient_avg_psi_per_ft,
        fracgradientavgcalc / 22.620593832021 as frac_gradient_avg_psi_per_ft,

        -- volumes (converted to barrels)
        {{ wv_cbm_to_bbl('volcleantotal') }} as volume_clean_total_bbl,
        {{ wv_cbm_to_bbl('volcleantotalcalc') }} as volume_clean_total_calc_bbl,
        {{ wv_cbm_to_bbl('volslurrytotal') }} as volume_slurry_total_bbl,
        {{ wv_cbm_to_bbl('volslurrytotalcalc') }} as volume_slurry_total_calc_bbl,
        {{ wv_cbm_to_bbl('volrecoveredtotal') }} as volume_recovered_total_bbl,
        {{ wv_cbm_to_bbl('volrecoveredtotalcalc') }} as volume_recovered_total_calc_bbl,
        {{ wv_cbm_to_bbl('volnetcleancalc') }} as total_clean_minus_recovered_volume_bbl,
        {{ wv_cbm_to_bbl('volnetslurrycalc') }} as total_slurry_minus_recovered_volume_bbl,
        {{ wv_cbm_to_bbl('volco2total') }} as volume_co2_total_bbl,
        {{ wv_cbm_to_bbl('volco2totalcalc') }} as volume_co2_total_calc_bbl,
        {{ wv_cbm_to_bbl('voln2total') }} as volume_n2_total_bbl,
        {{ wv_cbm_to_bbl('voln2totalcalc') }} as volume_n2_total_calc_bbl,

        -- volume per length (inline — uncommon conversion factor)
        volcleantotalperlengthcalc / 0.52161187664042 as volume_per_length_gross_bbl_per_ft,

        -- proppant mass (converted to pounds)
        {{ wv_kg_to_lb('massproptotal') }} as proppant_total_lb,
        {{ wv_kg_to_lb('massproptotalcalc') }} as proppant_total_calc_lb,
        {{ wv_kg_to_lb('massproptotalperdurnetcalc') }} as mass_proppant_per_day_lb,
        {{ wv_kg_to_lb('massproptotalperintnocalc') }} as mass_proppant_per_stage_lb,
        {{ wv_kg_to_lb('massproptotalperintperdurcalc') }} as mass_proppant_per_stage_per_day_lb,

        -- proppant linear density (converted to lb/ft)
        {{ wv_kgm_to_lb_per_ft('massproptotalperlengthcalc') }} as mass_proppant_per_gross_length_lb_per_ft,
        {{ wv_kgm_to_lb_per_ft('massproptotalperlengthsumcalc') }} as mass_proppant_per_net_length_lb_per_ft,

        -- proppant concentrations (converted to LB/GAL)
        {{ wv_kgm3_to_lb_per_gal('concbhavgcalc') }} as bh_conc_avg_all_stages_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhmaxcalc') }} as bh_conc_max_all_stages_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhmincalc') }} as bh_conc_min_all_stages_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfavgcalc') }} as surf_conc_avg_all_stages_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfmaxcalc') }} as surf_conc_max_all_stages_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfmincalc') }} as surf_conc_min_all_stages_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concintavgcalc') }} as conc_at_zone_avg_all_stages_lb_per_gal,

        -- performance metrics (inline — percent, no macro)
        fluidefficiency / 0.01 as fluid_efficiency_percent,

        -- pump power (converted to HP)
        {{ wv_watts_to_hp('powertotal') }} as pump_power_total_hp,

        -- temperatures (converted to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('temptreat') }} as treat_temperature_fahrenheit,
        {{ wv_celsius_to_fahrenheit('temptreatavgcalc') }} as treat_temperature_avg_fahrenheit,

        -- performance rates (converted to ft/hr)
        {{ wv_mps_to_ft_per_hr('lengthperdurnetcalc') }} as length_gross_per_hour_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('lengthsumperdurnetcalc') }} as length_net_per_hour_ft_per_hr,

        -- cost per length (converted to per-foot rate)
        {{ wv_per_meter_to_per_foot('costperlengthcalc') }} as cost_per_length_gross_per_ft,
        {{ wv_per_meter_to_per_foot('costperlengthsumcalc') }} as cost_per_length_net_per_ft,

        -- cost per proppant (inline — uncommon conversion)
        costpermassproptotalcalc / 2.20462262184878 as cost_per_mass_proppant_per_lb,

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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as stimulation_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        stimulation_sk,

        -- identifiers
        record_id,
        well_id,
        job_id,
        job_table_key,

        -- descriptive fields
        proposed_or_actual,
        stimulation_type,
        stimulation_subtype,
        icon_name,
        category,
        number_of_wells,
        stim_treat_company,
        stim_treat_supervisor,
        diversion_company,
        diversion_method,
        technical_result,
        tech_result_details,
        tech_result_note,
        user_text_1,
        comment,

        -- stage and interval information
        design_number_of_treatment_intervals,
        actual_total_stages,
        calculated_stages,
        total_number_of_clusters,
        clusters_per_stage,
        total_number_of_plugs,
        stages_per_day,
        plugs_per_day,

        -- cost calculations
        total_cost,
        cost_per_cluster,
        cost_per_stage,

        -- user numeric fields
        user_number_1,
        user_number_2,
        user_number_3,

        -- durations
        total_duration_gross_hours,
        total_duration_net_hours,
        total_pump_duration_calc_hours,
        total_pumping_hours_per_day,
        total_pump_duration_minutes,

        -- depths
        min_top_depth_ft,
        max_bottom_depth_ft,
        length_gross_ft,
        length_net_ft,

        -- treatment rates
        treat_rate_avg_bbl_per_min,
        treat_rate_max_bbl_per_min,
        clean_rate_avg_bbl_per_min,
        clean_rate_max_bbl_per_min,
        slurry_rate_avg_bbl_per_min,
        slurry_rate_max_bbl_per_min,

        -- pressures
        treat_pressure_avg_psi,
        treat_pressure_max_psi,
        treat_pressure_avg_all_stages_psi,
        treat_pressure_max_all_stages_psi,
        breakdown_pressure_avg_psi,
        closure_pressure_avg_psi,
        screen_out_pressure_avg_psi,

        -- pressure gradients
        closure_gradient_avg_psi_per_ft,
        frac_gradient_avg_psi_per_ft,

        -- volumes
        volume_clean_total_bbl,
        volume_clean_total_calc_bbl,
        volume_slurry_total_bbl,
        volume_slurry_total_calc_bbl,
        volume_recovered_total_bbl,
        volume_recovered_total_calc_bbl,
        total_clean_minus_recovered_volume_bbl,
        total_slurry_minus_recovered_volume_bbl,
        volume_co2_total_bbl,
        volume_co2_total_calc_bbl,
        volume_n2_total_bbl,
        volume_n2_total_calc_bbl,
        volume_per_length_gross_bbl_per_ft,

        -- proppant mass
        proppant_total_lb,
        proppant_total_calc_lb,
        mass_proppant_per_day_lb,
        mass_proppant_per_stage_lb,
        mass_proppant_per_stage_per_day_lb,

        -- proppant linear density
        mass_proppant_per_gross_length_lb_per_ft,
        mass_proppant_per_net_length_lb_per_ft,

        -- proppant concentrations
        bh_conc_avg_all_stages_lb_per_gal,
        bh_conc_max_all_stages_lb_per_gal,
        bh_conc_min_all_stages_lb_per_gal,
        surf_conc_avg_all_stages_lb_per_gal,
        surf_conc_max_all_stages_lb_per_gal,
        surf_conc_min_all_stages_lb_per_gal,
        conc_at_zone_avg_all_stages_lb_per_gal,

        -- performance metrics
        fluid_efficiency_percent,
        pump_power_total_hp,

        -- temperatures
        treat_temperature_fahrenheit,
        treat_temperature_avg_fahrenheit,

        -- performance rates
        length_gross_per_hour_ft_per_hr,
        length_net_per_hour_ft_per_hr,

        -- cost per length
        cost_per_length_gross_per_ft,
        cost_per_length_net_per_ft,
        cost_per_mass_proppant_per_lb,

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
