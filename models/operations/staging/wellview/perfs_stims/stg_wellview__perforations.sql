{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per perforation record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVPERFORATION') }}
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
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idreczoneor)::varchar as zone_id,
        trim(idreczoneortk)::varchar as zone_table_key,
        trim(idreczonecalc)::varchar as linked_zone_id,
        trim(idreczonecalctk)::varchar as linked_zone_table_key,
        trim(idrecstring)::varchar as string_perforated_id,
        trim(idrecstringtk)::varchar as string_perforated_table_key,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,
        trim(idreclog)::varchar as reference_log_id,
        trim(idreclogtk)::varchar as reference_log_table_key,
        trim(idrecjobprogramphasecalc)::varchar as phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as phase_table_key,
        trim(idreclastcompletioncalc)::varchar as last_completion_id,
        trim(idreclastcompletioncalctk)::varchar as last_completion_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idrecotherinholecalc)::varchar as other_in_hole_id,
        trim(idrecotherinholecalctk)::varchar as other_in_hole_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(typ)::varchar as perforation_type,
        trim(conveymeth)::varchar as conveyance_method,
        trim(chargetyp)::varchar as charge_type,
        trim(chargemake)::varchar as charge_make,
        trim(explosivetyp)::varchar as explosive_type,
        trim(orientation)::varchar as orientation,
        trim(orientmethod)::varchar as orientation_method,
        trim(balance)::varchar as over_under_balanced,
        trim(fluidtyp)::varchar as fluid_type,
        trim(presbhtyp)::varchar as bh_pressure_type,
        trim(currentstatuscalc)::varchar as current_status,
        trim(statusprimarycalc)::varchar as open_or_closed,
        trim(resulttechnical)::varchar as technical_result,
        trim(resulttechnicaldetail)::varchar as tech_result_details,
        trim(resulttechnicalnote)::varchar as tech_result_note,
        trim(formationcalc)::varchar as formation,
        trim(reservoircalc)::varchar as reservoir,
        trim(contractor)::varchar as perforation_company,
        trim(icondrawshort)::varchar as draw_short,
        trim(com)::varchar as comment,
        intno::float as stage_number,
        cluserrefno::float as cluster_reference_number,

        -- gun specifications
        trim(gundes)::varchar as gun_description,
        trim(gunmetallurgy)::varchar as gun_metallurgy,
        trim(guncentralize)::varchar as gun_centralize,
        trim(gunleftinhole)::varchar as gun_left_in_hole,
        trim(carrierdes)::varchar as carrier_description,
        trim(carriermake)::varchar as carrier_make,
        phasing::float as phasing_degrees,

        -- shot information
        shotplan::float as shots_planned,
        shottotal::float as entered_shot_total,
        shotstotalcalc::float as calculated_shot_total,
        shotstotalaltcalc::float as calculated_shot_total_alt,
        shotsmisfirecalc::float as shots_misfired,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtoptobtmcalc') }} as perforation_interval_thickness_ft,
        {{ wv_meters_to_feet('depthmppcalc') }} as depth_mpp_ft,
        {{ wv_meters_to_feet('depthcsngcollarref') }} as collar_ref_depth_ft,
        {{ wv_meters_to_feet('depthtvdcsngcollarrefcalc') }} as collar_ref_depth_tvd_ft,
        {{ wv_meters_to_feet('distancereftotopcalc') }} as distance_ref_to_top_ft,
        {{ wv_meters_to_feet('depthgauge') }} as gauge_depth_ft,
        {{ wv_meters_to_feet('depthtvdgaugecalc') }} as gauge_depth_tvd_ft,
        {{ wv_meters_to_feet('depthfluidbefore') }} as fluid_depth_before_shot_ft,
        {{ wv_meters_to_feet('depthfluidafter') }} as fluid_depth_after_shot_ft,
        {{ wv_meters_to_feet('depthtvdfluidbeforecalc') }} as fluid_depth_before_shot_tvd_ft,
        {{ wv_meters_to_feet('depthtvdfluidaftercalc') }} as fluid_depth_after_shot_tvd_ft,

        -- shot density (per-foot rate)
        {{ wv_per_meter_to_per_foot('shotdensity') }} as shot_density_shots_per_ft,
        {{ wv_per_meter_to_per_foot('shotstotalaltperdensitycalc') }} as shots_total_per_density_per_ft,

        -- gun and charge sizes (converted to inches)
        {{ wv_meters_to_inches('szgun') }} as gun_size_inches,
        {{ wv_meters_to_inches('szholeact') }} as estimated_actual_hole_diameter_inches,
        {{ wv_meters_to_inches('szholenom') }} as nominal_hole_diameter_inches,
        {{ wv_meters_to_inches('penetrationnom') }} as nominal_penetration_inches,

        -- charge size (grams — inline, no macro)
        chargesz / 0.001 as charge_size_grams,

        -- pressures (converted to PSI)
        {{ wv_kpa_to_psi('balancepres') }} as over_under_pressure_psi,
        {{ wv_kpa_to_psi('presdesignbh') }} as design_bh_pressure_psi,
        {{ wv_kpa_to_psi('presinitsurf') }} as initial_surface_pressure_psi,
        {{ wv_kpa_to_psi('presfinalsurf') }} as final_surface_pressure_psi,
        {{ wv_kpa_to_psi('presbh') }} as bottom_hole_pressure_psi,
        {{ wv_kpa_to_psi('presdatum') }} as datum_pressure_psi,
        {{ wv_kpa_to_psi('presmpp') }} as mpp_pressure_psi,
        {{ wv_kpa_to_psi('presduringperf') }} as pressure_during_perforation_psi,
        {{ wv_kpa_to_psi('presbhtodatumcalc') }} as pressure_bh_to_datum_psi,
        {{ wv_kpa_to_psi('presbhtomppcalc') }} as pressure_bh_to_mpp_psi,
        {{ wv_kpa_to_psi('preshhsurftompp') }} as estimated_hh_surf_to_mpp_psi,
        {{ wv_kpa_to_psi('presbhsitphhcalc') }} as bh_pressure_for_sitp_hh_psi,

        -- pressure gradient (psi/ft — inline, no macro)
        presgradientgaugetompp / 22.620593832021 as pressure_gradient_gauge_to_mpp_psi_per_ft,

        -- fluid density
        {{ wv_kgm3_to_lb_per_gal('fluiddensity') }} as fluid_density_lb_per_gal,

        -- fluid rates
        {{ wv_mps_to_ft_per_hr('ratefluidbefore') }} as fluid_rate_before_shot_ft_per_hr,
        {{ wv_mps_to_ft_per_hr('ratefluidafter') }} as fluid_rate_after_shot_ft_per_hr,

        -- volumes (converted to barrels)
        {{ wv_cbm_to_bbl('usernum1') }} as vol_fluid_bbl,

        -- dates
        dttm::timestamp_ntz as perforation_date,
        dttmstatuscalc::timestamp_ntz as current_status_date,

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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as perforation_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        perforation_sk,

        -- identifiers
        record_id,
        well_id,
        wellbore_id,
        wellbore_table_key,
        zone_id,
        zone_table_key,
        linked_zone_id,
        linked_zone_table_key,
        string_perforated_id,
        string_perforated_table_key,
        job_id,
        job_table_key,
        reference_log_id,
        reference_log_table_key,
        phase_id,
        phase_table_key,
        last_completion_id,
        last_completion_table_key,
        last_rig_id,
        last_rig_table_key,
        other_in_hole_id,
        other_in_hole_table_key,

        -- descriptive fields
        proposed_or_actual,
        perforation_type,
        conveyance_method,
        charge_type,
        charge_make,
        explosive_type,
        orientation,
        orientation_method,
        over_under_balanced,
        fluid_type,
        bh_pressure_type,
        current_status,
        open_or_closed,
        technical_result,
        tech_result_details,
        tech_result_note,
        formation,
        reservoir,
        perforation_company,
        draw_short,
        comment,
        stage_number,
        cluster_reference_number,

        -- gun specifications
        gun_description,
        gun_metallurgy,
        gun_centralize,
        gun_left_in_hole,
        carrier_description,
        carrier_make,
        phasing_degrees,

        -- shot information
        shots_planned,
        entered_shot_total,
        calculated_shot_total,
        calculated_shot_total_alt,
        shots_misfired,

        -- depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        perforation_interval_thickness_ft,
        depth_mpp_ft,
        collar_ref_depth_ft,
        collar_ref_depth_tvd_ft,
        distance_ref_to_top_ft,
        gauge_depth_ft,
        gauge_depth_tvd_ft,
        fluid_depth_before_shot_ft,
        fluid_depth_after_shot_ft,
        fluid_depth_before_shot_tvd_ft,
        fluid_depth_after_shot_tvd_ft,

        -- shot density
        shot_density_shots_per_ft,
        shots_total_per_density_per_ft,

        -- gun and charge sizes
        gun_size_inches,
        estimated_actual_hole_diameter_inches,
        nominal_hole_diameter_inches,
        nominal_penetration_inches,
        charge_size_grams,

        -- pressures
        over_under_pressure_psi,
        design_bh_pressure_psi,
        initial_surface_pressure_psi,
        final_surface_pressure_psi,
        bottom_hole_pressure_psi,
        datum_pressure_psi,
        mpp_pressure_psi,
        pressure_during_perforation_psi,
        pressure_bh_to_datum_psi,
        pressure_bh_to_mpp_psi,
        estimated_hh_surf_to_mpp_psi,
        bh_pressure_for_sitp_hh_psi,
        pressure_gradient_gauge_to_mpp_psi_per_ft,

        -- fluid measurements
        fluid_density_lb_per_gal,
        fluid_rate_before_shot_ft_per_hr,
        fluid_rate_after_shot_ft_per_hr,

        -- volumes
        vol_fluid_bbl,

        -- dates
        perforation_date,
        current_status_date,

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
