{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPMANDREL') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as mandrel_id,
        trim(idrecparent)::varchar as tubing_component_id,
        trim(idwell)::varchar as well_id,

        -- descriptive fields
        stationno::float as station_number,

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

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and mandrel_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['mandrel_id']) }} as mandrel_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        mandrel_sk,

        -- identifiers
        mandrel_id,
        tubing_component_id,
        well_id,

        -- descriptive fields
        station_number,

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
