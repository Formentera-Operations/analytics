{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIGPUMPOP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as pump_operation_id,
        trim(idrecparent)::varchar as rig_pump_id,
        trim(idwell)::varchar as well_id,

        -- operational period
        dttmstart::timestamp_ntz as operation_start_datetime,
        dttmend::timestamp_ntz as operation_end_datetime,

        -- pump configuration (converted from meters to inches)
        {{ wv_meters_to_inches('szliner') }} as liner_size_in,

        -- pressure (converted from kPa to PSI)
        {{ wv_kpa_to_psi('pressuremax') }} as maximum_pressure_psi,

        -- volume per stroke (converted from m3 to bbl)
        {{ wv_cbm_to_bbl('volperstroke') }} as volume_per_stroke_override_bbl_per_stroke,
        {{ wv_cbm_to_bbl('volperstrokecalc') }} as volume_per_stroke_calculated_bbl_per_stroke,

        -- operational time tracking (converted from days to hours)
        {{ wv_days_to_hours('tmcirccalc') }} as circulating_time_hours,
        {{ wv_days_to_hours('tmdrillcalc') }} as drilling_time_hours,

        -- performance metrics (proportion -> percentage)
        volefficiencycalc / 0.01 as volumetric_efficiency_percent,

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
        and pump_operation_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['pump_operation_id']) }} as rig_mud_pump_operation_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rig_mud_pump_operation_sk,

        -- identifiers
        pump_operation_id,
        rig_pump_id,
        well_id,

        -- operational period
        operation_start_datetime,
        operation_end_datetime,

        -- pump configuration
        liner_size_in,

        -- pressure
        maximum_pressure_psi,

        -- volume per stroke
        volume_per_stroke_override_bbl_per_stroke,
        volume_per_stroke_calculated_bbl_per_stroke,

        -- operational time tracking
        circulating_time_hours,
        drilling_time_hours,

        -- performance metrics
        volumetric_efficiency_percent,

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
