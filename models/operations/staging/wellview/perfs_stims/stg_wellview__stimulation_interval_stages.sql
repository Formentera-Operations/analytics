{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per stim stage)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTSTG') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as stage_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as stimulation_interval_id,
        trim(idrecfluid)::varchar as fluid_id,
        trim(idrecfluidtk)::varchar as fluid_table_key,
        trim(idrecprop)::varchar as proppant_id,
        trim(idrecproptk)::varchar as proppant_table_key,

        -- descriptive fields
        stagenum::int as stage_number,
        trim(stagetyp1)::varchar as stage_type,
        trim(stagetyp2)::varchar as stage_sub_type,
        trim(des)::varchar as description,
        trim(gasdes)::varchar as gas_description,
        trim(com)::varchar as comment,
        trim(usertxt1)::varchar as user_text_1,
        ballsnoused::int as balls_used,
        exclude::boolean as exclude_from_calculations,

        -- duration (days → minutes)
        {{ wv_days_to_minutes('durpumptotal') }} as pump_duration_minutes,

        -- treatment rates — surface pump (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpsurfstart') }} as pump_rate_surf_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpsurfavg') }} as pump_rate_surf_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpsurfend') }} as pump_rate_surf_end_bbl_per_min,

        -- treatment rates — bottom hole pump (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpbhstart') }} as pump_rate_bh_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpbhavg') }} as pump_rate_bh_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpbhend') }} as pump_rate_bh_end_bbl_per_min,

        -- treatment rates — clean fluid (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanstart') }} as clean_rate_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanmax') }} as clean_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratecleanmin') }} as clean_rate_min_bbl_per_min,

        -- treatment rates — slurry (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurrystart') }} as slurry_rate_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurryavg') }} as slurry_rate_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurrymax') }} as slurry_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurrymin') }} as slurry_rate_min_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateslurryend') }} as slurry_rate_end_bbl_per_min,

        -- treatment rates — CO2 surface (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2surfstart') }} as co2_rate_surf_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2surfavg') }} as co2_rate_surf_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2surfend') }} as co2_rate_surf_end_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2max') }} as co2_rate_max_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2min') }} as co2_rate_min_bbl_per_min,

        -- treatment rates — CO2 foam (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2foamstart') }} as co2_foam_rate_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('rateco2foamend') }} as co2_foam_rate_end_bbl_per_min,

        -- treatment rates — N2 surface (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('raten2surfstart') }} as n2_rate_surf_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('raten2surfavg') }} as n2_rate_surf_avg_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('raten2surfend') }} as n2_rate_surf_end_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('raten2max') }} as n2_rate_max_bbl_per_min,

        -- treatment rates — N2 foam (converted to BBL/MIN)
        {{ wv_cbm_per_sec_to_bbl_per_min('raten2foamstart') }} as n2_foam_rate_start_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('raten2foamend') }} as n2_foam_rate_end_bbl_per_min,

        -- surface pressures (converted to PSI)
        {{ wv_kpa_to_psi('pressurfstart') }} as surface_pressure_start_psi,
        {{ wv_kpa_to_psi('pressurfavg') }} as surface_pressure_avg_psi,
        {{ wv_kpa_to_psi('pressurfmax') }} as surface_pressure_max_psi,
        {{ wv_kpa_to_psi('pressurfmin') }} as surface_pressure_min_psi,
        {{ wv_kpa_to_psi('pressurfend') }} as surface_pressure_end_psi,
        {{ wv_kpa_to_psi('pressurfmaxmincalc') }} as surface_pressure_max_minus_min_psi,

        -- bottom hole pressures (converted to PSI)
        {{ wv_kpa_to_psi('presbhstart') }} as bh_pressure_start_psi,
        {{ wv_kpa_to_psi('presbhavg') }} as bh_pressure_avg_psi,
        {{ wv_kpa_to_psi('presbhend') }} as bh_pressure_end_psi,
        {{ wv_kpa_to_psi('presbhmin') }} as bh_pressure_min_psi,
        {{ wv_kpa_to_psi('presbhmaxmincalc') }} as bh_pressure_max_minus_min_psi,

        -- zone pressures (converted to PSI)
        {{ wv_kpa_to_psi('preszonemax') }} as zone_pressure_max_psi,
        {{ wv_kpa_to_psi('prespumpingmax') }} as pumping_pressure_max_psi,

        -- surface concentrations (converted to LB/GAL)
        {{ wv_kgm3_to_lb_per_gal('concsurfstart') }} as surf_conc_start_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfmax') }} as surf_conc_max_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfmin') }} as surf_conc_min_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concsurfend') }} as surf_conc_end_lb_per_gal,

        -- bottom hole concentrations (converted to LB/GAL)
        {{ wv_kgm3_to_lb_per_gal('concbhstart') }} as bh_conc_start_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhavg') }} as bh_conc_avg_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhmax') }} as bh_conc_max_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhmin') }} as bh_conc_min_lb_per_gal,
        {{ wv_kgm3_to_lb_per_gal('concbhend') }} as bh_conc_end_lb_per_gal,

        -- volumes — slurry (converted to barrels)
        {{ wv_cbm_to_bbl('volslurry') }} as volume_slurry_bbl,
        {{ wv_cbm_to_bbl('volslurrycumcalc') }} as volume_slurry_cumulative_bbl,

        -- volumes — clean (converted to barrels)
        {{ wv_cbm_to_bbl('volclean') }} as volume_clean_bbl,
        {{ wv_cbm_to_bbl('volcleancumcalc') }} as volume_clean_cumulative_bbl,
        {{ wv_cbm_to_bbl('volnetcleancalc') }} as volume_net_clean_bbl,
        {{ wv_cbm_to_bbl('volnetslurrycalc') }} as volume_net_slurry_bbl,

        -- volumes — recovered (converted to barrels)
        {{ wv_cbm_to_bbl('volrecoveredcumcalc') }} as volume_recovered_cumulative_bbl,

        -- volumes — CO2 and N2 (converted to barrels)
        {{ wv_cbm_to_bbl('volco2') }} as volume_co2_bbl,
        {{ wv_cbm_to_bbl('volco2cumcalc') }} as volume_co2_cumulative_bbl,
        {{ wv_cbm_to_bbl('voln2') }} as volume_n2_bbl,
        {{ wv_cbm_to_bbl('voln2cumcalc') }} as volume_n2_cumulative_bbl,

        -- proppant mass (converted to pounds)
        {{ wv_kg_to_lb('massprop') }} as proppant_mass_lb,
        {{ wv_kg_to_lb('masspropwh') }} as proppant_mass_at_wellhead_lb,
        {{ wv_kg_to_lb('masspropcumcalc') }} as proppant_mass_cumulative_lb,

        -- pump power (converted to HP)
        {{ wv_watts_to_hp('pumppoweravg') }} as pump_power_avg_hp,

        -- temperatures (converted to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('temptreatavg') }} as treat_temperature_avg_fahrenheit,

        -- foam quality and friction (dimensionless — inline, no macro)
        foamco2quality / 0.01 as foam_co2_quality_percent,
        foamn2quality / 0.01 as foam_n2_quality_percent,
        frictionfactor::float as friction_factor,
        spacefactor::float as space_factor,

        -- user numeric fields
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,
        usernum4::float as user_number_4,
        usernum5::float as user_number_5,

        -- dates
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,

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
        and stage_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['stage_id']) }} as stim_stage_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        stim_stage_sk,

        -- identifiers
        stage_id,
        well_id,
        stimulation_interval_id,
        fluid_id,
        fluid_table_key,
        proppant_id,
        proppant_table_key,

        -- descriptive fields
        stage_number,
        stage_type,
        stage_sub_type,
        description,
        gas_description,
        comment,
        user_text_1,
        balls_used,
        exclude_from_calculations,

        -- duration
        pump_duration_minutes,

        -- treatment rates — surface pump
        pump_rate_surf_start_bbl_per_min,
        pump_rate_surf_avg_bbl_per_min,
        pump_rate_surf_end_bbl_per_min,

        -- treatment rates — bottom hole pump
        pump_rate_bh_start_bbl_per_min,
        pump_rate_bh_avg_bbl_per_min,
        pump_rate_bh_end_bbl_per_min,

        -- treatment rates — clean fluid
        clean_rate_start_bbl_per_min,
        clean_rate_max_bbl_per_min,
        clean_rate_min_bbl_per_min,

        -- treatment rates — slurry
        slurry_rate_start_bbl_per_min,
        slurry_rate_avg_bbl_per_min,
        slurry_rate_max_bbl_per_min,
        slurry_rate_min_bbl_per_min,
        slurry_rate_end_bbl_per_min,

        -- treatment rates — CO2
        co2_rate_surf_start_bbl_per_min,
        co2_rate_surf_avg_bbl_per_min,
        co2_rate_surf_end_bbl_per_min,
        co2_rate_max_bbl_per_min,
        co2_rate_min_bbl_per_min,
        co2_foam_rate_start_bbl_per_min,
        co2_foam_rate_end_bbl_per_min,

        -- treatment rates — N2
        n2_rate_surf_start_bbl_per_min,
        n2_rate_surf_avg_bbl_per_min,
        n2_rate_surf_end_bbl_per_min,
        n2_rate_max_bbl_per_min,
        n2_foam_rate_start_bbl_per_min,
        n2_foam_rate_end_bbl_per_min,

        -- surface pressures
        surface_pressure_start_psi,
        surface_pressure_avg_psi,
        surface_pressure_max_psi,
        surface_pressure_min_psi,
        surface_pressure_end_psi,
        surface_pressure_max_minus_min_psi,

        -- bottom hole pressures
        bh_pressure_start_psi,
        bh_pressure_avg_psi,
        bh_pressure_end_psi,
        bh_pressure_min_psi,
        bh_pressure_max_minus_min_psi,

        -- zone and pumping pressures
        zone_pressure_max_psi,
        pumping_pressure_max_psi,

        -- surface concentrations
        surf_conc_start_lb_per_gal,
        surf_conc_max_lb_per_gal,
        surf_conc_min_lb_per_gal,
        surf_conc_end_lb_per_gal,

        -- bottom hole concentrations
        bh_conc_start_lb_per_gal,
        bh_conc_avg_lb_per_gal,
        bh_conc_max_lb_per_gal,
        bh_conc_min_lb_per_gal,
        bh_conc_end_lb_per_gal,

        -- volumes
        volume_slurry_bbl,
        volume_slurry_cumulative_bbl,
        volume_clean_bbl,
        volume_clean_cumulative_bbl,
        volume_net_clean_bbl,
        volume_net_slurry_bbl,
        volume_recovered_cumulative_bbl,
        volume_co2_bbl,
        volume_co2_cumulative_bbl,
        volume_n2_bbl,
        volume_n2_cumulative_bbl,

        -- proppant mass
        proppant_mass_lb,
        proppant_mass_at_wellhead_lb,
        proppant_mass_cumulative_lb,

        -- pump power
        pump_power_avg_hp,

        -- temperatures
        treat_temperature_avg_fahrenheit,

        -- foam quality and friction
        foam_co2_quality_percent,
        foam_n2_quality_percent,
        friction_factor,
        space_factor,

        -- user numeric fields
        user_number_1,
        user_number_2,
        user_number_3,
        user_number_4,
        user_number_5,

        -- dates
        start_date,
        end_date,

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
