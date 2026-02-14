{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per cement stage fluid)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVCEMENTSTAGEFLUID') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as cement_stage_fluid_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as cement_stage_id,
        sysseq::float as sequence_number,

        -- fluid classification
        trim(typ)::varchar as fluid_type,
        trim(objective)::varchar as fluid_objective,
        trim(cmtclass)::varchar as cement_class,
        trim(watersource)::varchar as water_source,

        -- descriptive fields
        trim(desfluid)::varchar as fluid_description,
        trim(com)::varchar as comments,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtopest') }} as estimated_top_depth_ft,
        {{ wv_meters_to_feet('depthbtmest') }} as estimated_bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopestcalc') }} as estimated_top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmestcalc') }} as estimated_bottom_depth_tvd_ft,

        -- cement quantities (kg to sacks — keep inline)
        amtcement / 45.359237 as cement_amount_sacks,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volpumped') }} as volume_pumped_bbl,

        -- percentages (keep inline)
        excesspumped / 0.01 as excess_pumped_percent,
        freewater / 0.01 as free_water_percent,

        -- mix design properties (specialized conversions — keep inline)
        mixwaterratio / 8.34540445201933E-05 as mix_water_ratio_gal_per_sack,
        yield / 0.000624279605761446 as cement_yield_ft3_per_sack,

        -- density (converted from metric to US units)
        {{ wv_kgm3_to_lb_per_gal('density') }} as fluid_density_ppg,

        -- rheological properties (converted from metric to US units)
        {{ wv_pas_to_cp('plasticvis') }} as plastic_viscosity_cp,
        yieldpt / 0.000478802589803 as yield_point_lbf_per_100ft2,

        -- filtrate (specialized conversion — keep inline)
        filtrate / 4.8E-05 as filtrate_loss_ml_per_30min,

        -- temperatures (converted from metric to US units)
        {{ wv_celsius_to_fahrenheit('tempvisc') }} as fan_temperature_f,
        {{ wv_celsius_to_fahrenheit('thickentemp') }} as thickening_temperature_f,
        {{ wv_celsius_to_fahrenheit('comprstrtemp') }} as compressive_strength_test_temp_f,

        -- time (converted from metric to US units)
        {{ wv_days_to_hours('thickentm') }} as thickening_time_hours,
        {{ wv_days_to_hours('comprstrtm1') }} as first_compressive_strength_test_time_hours,
        {{ wv_days_to_hours('comprstrtm2') }} as second_compressive_strength_test_time_hours,

        -- pressures (converted from metric to US units)
        {{ wv_kpa_to_psi('presfinal') }} as final_pressure_psi,
        {{ wv_kpa_to_psi('presfrictionloss') }} as friction_pressure_loss_psi,
        {{ wv_kpa_to_psi('comprstr1') }} as first_compressive_strength_psi,
        {{ wv_kpa_to_psi('comprstr2') }} as second_compressive_strength_psi,

        -- pump rate (converted from metric to US units)
        {{ wv_cbm_per_sec_to_bbl_per_min('rateavg') }} as average_pump_rate_bbl_per_min,

        -- dates
        dttmmix::timestamp_ntz as mix_datetime,
        dttmstartpump::timestamp_ntz as pump_start_datetime,
        dttmendpump::timestamp_ntz as pump_end_datetime,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
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

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and cement_stage_fluid_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['cement_stage_fluid_id']) }} as cement_stage_fluid_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        cement_stage_fluid_sk,

        -- identifiers
        cement_stage_fluid_id,
        well_id,
        cement_stage_id,
        sequence_number,

        -- fluid classification
        fluid_type,
        fluid_objective,
        cement_class,
        water_source,

        -- descriptive fields
        fluid_description,
        comments,

        -- depths
        estimated_top_depth_ft,
        estimated_bottom_depth_ft,
        estimated_top_depth_tvd_ft,
        estimated_bottom_depth_tvd_ft,

        -- measurements
        cement_amount_sacks,
        volume_pumped_bbl,
        excess_pumped_percent,
        free_water_percent,

        -- mix design properties
        mix_water_ratio_gal_per_sack,
        cement_yield_ft3_per_sack,

        -- density
        fluid_density_ppg,

        -- rheological properties
        plastic_viscosity_cp,
        yield_point_lbf_per_100ft2,
        filtrate_loss_ml_per_30min,

        -- temperatures
        fan_temperature_f,
        thickening_temperature_f,
        compressive_strength_test_temp_f,

        -- time
        thickening_time_hours,
        first_compressive_strength_test_time_hours,
        second_compressive_strength_test_time_hours,

        -- pressures
        final_pressure_psi,
        friction_pressure_loss_psi,
        first_compressive_strength_psi,
        second_compressive_strength_psi,

        -- pump rate
        average_pump_rate_bbl_per_min,

        -- dates
        mix_datetime,
        pump_start_datetime,
        pump_end_datetime,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
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
