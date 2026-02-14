{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per cement stage)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVCEMENTSTAGE') }}
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

        -- stage information
        stagenum::float as stage_number,
        trim(des)::varchar as description,
        trim(objective)::varchar as objective,
        trim(measmethod)::varchar as top_measurement_method,
        trim(topplug)::varchar as top_plug,
        trim(btmplug)::varchar as bottom_plug,
        trim(tagmethod)::varchar as tag_method,
        trim(pipemovenote)::varchar as pipe_movement_note,
        trim(com)::varchar as comment,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('recipstroke') }} as reciprocation_stroke_length_ft,
        {{ wv_meters_to_feet('depthtagged') }} as tagged_depth_ft,
        {{ wv_meters_to_feet('depthdrillout') }} as depth_plug_drilled_out_to_ft,

        -- pump rates (converted from metric to US units)
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpstart') }} as initial_pump_rate_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpend') }} as final_pump_rate_bbl_per_min,
        {{ wv_cbm_per_sec_to_bbl_per_min('ratepumpavg') }} as avg_pump_rate_bbl_per_min,

        -- pressures (converted from metric to US units)
        {{ wv_kpa_to_psi('prespumpend') }} as final_pump_pressure_psi,
        {{ wv_kpa_to_psi('presplugbump') }} as plug_bump_pressure_psi,
        {{ wv_kpa_to_psi('presheld') }} as pressure_held_psi,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volreturncmnt') }} as cement_volume_return_bbl,
        {{ wv_cbm_to_bbl('vollost') }} as volume_lost_bbl,
        {{ wv_cbm_to_bbl('volinfrm') }} as volume_squeezed_in_to_formation_bbl,

        -- weights (converted from metric to US units)
        {{ wv_newtons_to_klbf('weighttagged') }} as tag_weight_1000_lbf,

        -- sizes (converted from metric to US units)
        {{ wv_meters_to_inches('oddrillout') }} as drill_out_diameter_inches,

        -- pipe movement
        reciprate::float as reciprocation_rate_spm,
        rotaterpm::float as pipe_rpm,

        -- duration (converted from metric to US units)
        {{ wv_days_to_hours('durdrillouttopumpendcalc') }} as drill_out_to_pump_end_duration_hours,

        -- operational flags
        plugfailed::boolean as plug_failed,
        floatfailed::boolean as float_failed,
        fullreturn::boolean as full_return,
        reciprocated::boolean as pipe_reciprocated,
        rotated::boolean as pipe_rotated,

        -- dates
        dttmstartpump::timestamp_ntz as pump_start_date,
        dttmendpump::timestamp_ntz as pump_end_date,
        dttmreleasedpres::timestamp_ntz as pressure_release_date,
        dttmtagged::timestamp_ntz as tag_cement_date,
        dttmdrillout::timestamp_ntz as drill_out_date,
        dttmpropdrillout::timestamp_ntz as proposed_drill_out_date,

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
        and record_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as cement_stage_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        cement_stage_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,

        -- stage information
        stage_number,
        description,
        objective,
        top_measurement_method,
        top_plug,
        bottom_plug,
        tag_method,
        pipe_movement_note,
        comment,

        -- depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        reciprocation_stroke_length_ft,
        tagged_depth_ft,
        depth_plug_drilled_out_to_ft,

        -- pump rates
        initial_pump_rate_bbl_per_min,
        final_pump_rate_bbl_per_min,
        avg_pump_rate_bbl_per_min,

        -- pressures
        final_pump_pressure_psi,
        plug_bump_pressure_psi,
        pressure_held_psi,

        -- volumes
        cement_volume_return_bbl,
        volume_lost_bbl,
        volume_squeezed_in_to_formation_bbl,

        -- weights
        tag_weight_1000_lbf,

        -- sizes
        drill_out_diameter_inches,

        -- pipe movement
        reciprocation_rate_spm,
        pipe_rpm,

        -- duration
        drill_out_to_pump_end_duration_hours,

        -- flags
        plug_failed,
        float_failed,
        full_return,
        pipe_reciprocated,
        pipe_rotated,

        -- dates
        pump_start_date,
        pump_end_date,
        pressure_release_date,
        tag_cement_date,
        drill_out_date,
        proposed_drill_out_date,

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
