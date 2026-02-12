{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMPPLUNGERENTRY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as plunger_entry_id,
        trim(idrecparent)::varchar as plunger_lift_id,
        trim(idflownet)::varchar as flow_network_id,

        -- dates
        dttm::timestamp_ntz as entry_date,

        -- operating parameters
        {{ pv_kpa_to_psi('plungeronpressure') }} as plunger_on_pressure_psi,

        -- trip counts
        tripcounttotal::float as total_trips,
        tripcountsuccess::float as successful_trips,
        tripcountfail::float as failed_trips,

        -- duration metrics
        {{ pv_seconds_to_minutes('duron') }} as duration_on_minutes,
        {{ pv_seconds_to_minutes('duroff') }} as duration_off_minutes,
        {{ pv_days_to_hours('afterflowtime') }} as after_flow_time_hours,

        -- travel time metrics
        {{ pv_seconds_to_minutes('traveltimeavg') }} as travel_time_avg_minutes,
        {{ pv_seconds_to_minutes('traveltimemax') }} as travel_time_max_minutes,
        {{ pv_seconds_to_minutes('traveltimemin') }} as travel_time_min_minutes,
        {{ pv_seconds_to_minutes('traveltimetarget') }} as travel_time_target_minutes,

        -- descriptive fields
        trim(com)::varchar as comments,

        -- user-defined fields
        trim(usertxt1)::varchar as plunger_make,
        trim(usertxt2)::varchar as plunger_model,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,
        userdttm1::timestamp_ntz as plunger_inspection_date,
        userdttm2::timestamp_ntz as plunger_replace_date,
        userdttm3::timestamp_ntz as user_date_3,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as record_tag,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,

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
        and plunger_entry_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['plunger_entry_id']) }} as plunger_reading_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        plunger_reading_sk,

        -- identifiers
        plunger_entry_id,
        plunger_lift_id,
        flow_network_id,

        -- dates
        entry_date,

        -- operating parameters
        plunger_on_pressure_psi,

        -- trip counts
        total_trips,
        successful_trips,
        failed_trips,

        -- duration metrics
        duration_on_minutes,
        duration_off_minutes,
        after_flow_time_hours,

        -- travel time metrics
        travel_time_avg_minutes,
        travel_time_max_minutes,
        travel_time_min_minutes,
        travel_time_target_minutes,

        -- descriptive fields
        comments,

        -- user-defined fields
        plunger_make,
        plunger_model,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,
        plunger_inspection_date,
        plunger_replace_date,
        user_date_3,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        record_tag,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
