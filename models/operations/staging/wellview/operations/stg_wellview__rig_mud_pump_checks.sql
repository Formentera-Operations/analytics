{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIGPUMPCHK') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as pump_check_id,
        trim(idrecparent)::varchar as rig_pump_id,
        trim(idwell)::varchar as well_id,

        -- check timing and location
        dttm::timestamp_ntz as check_datetime,
        {{ wv_meters_to_feet('depth') }} as depth_ft,

        -- pump operation parameters
        spm::float as strokes_per_minute,
        trim(pumpingmode)::varchar as pumping_mode,
        coalesce(slowspeed = 1, false) as is_slow_speed_check,

        -- performance metrics (converted to percentage and gpm)
        volefficiency / 0.01 as volumetric_efficiency_percent,
        {{ wv_cbm_per_sec_to_gpm('flowratecalc') }} as calculated_flow_rate_gpm,

        -- pressure readings (converted from kPa to PSI)
        {{ wv_kpa_to_psi('pres') }} as pressure_psi,
        {{ wv_kpa_to_psi('presclf') }} as choke_line_friction_pressure_psi,
        {{ wv_kpa_to_psi('presklf') }} as kill_line_friction_pressure_psi,

        -- related data references
        trim(idreclastmudchkcalc)::varchar as last_mud_check_id,
        trim(idreclastmudchkcalctk)::varchar as last_mud_check_table_key,

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
        and pump_check_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['pump_check_id']) }} as rig_mud_pump_check_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rig_mud_pump_check_sk,

        -- identifiers
        pump_check_id,
        rig_pump_id,
        well_id,

        -- check timing and location
        check_datetime,
        depth_ft,

        -- pump operation parameters
        strokes_per_minute,
        pumping_mode,
        is_slow_speed_check,

        -- performance metrics
        volumetric_efficiency_percent,
        calculated_flow_rate_gpm,

        -- pressure readings
        pressure_psi,
        choke_line_friction_pressure_psi,
        kill_line_friction_pressure_psi,

        -- related data references
        last_mud_check_id,
        last_mud_check_table_key,

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
