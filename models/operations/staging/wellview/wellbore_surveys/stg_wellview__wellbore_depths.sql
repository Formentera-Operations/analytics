{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'wellbore_surveys']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREKEYDEPTH') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as key_depth_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as wellbore_id,

        -- key depth classification
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(keydepthtyp)::varchar as key_depth_type,
        trim(typ1)::varchar as key_depth_category,
        trim(typ2)::varchar as key_depth_subcategory,
        trim(des)::varchar as key_depth_description,
        trim(method)::varchar as measurement_method,

        -- dates
        dttm::timestamp_ntz as key_depth_date,

        -- depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('lengthcalc') }} as interval_length_ft,

        -- geographic coordinates
        latitude::float as latitude_degrees,
        longitude::float as longitude_degrees,
        trim(latlongsource)::varchar as lat_long_data_source,
        utmx::float as utm_easting_meters,
        utmy::float as utm_northing_meters,
        utmgridzone::int as utm_grid_zone,
        trim(utmsource)::varchar as utm_data_source,

        -- flags
        coalesce(exclude = 1, false)::boolean as exclude_from_cost_calculations,

        -- comments
        trim(com)::varchar as comment,

        -- system locking
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as system_tag,

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
        and key_depth_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['key_depth_id']) }} as key_depth_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        key_depth_sk,

        -- identifiers
        key_depth_id,
        well_id,
        wellbore_id,

        -- key depth classification
        proposed_or_actual,
        key_depth_type,
        key_depth_category,
        key_depth_subcategory,
        key_depth_description,
        measurement_method,

        -- dates
        key_depth_date,

        -- depths
        top_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_ft,
        bottom_depth_tvd_ft,
        interval_length_ft,

        -- geographic coordinates
        latitude_degrees,
        longitude_degrees,
        lat_long_data_source,
        utm_easting_meters,
        utm_northing_meters,
        utm_grid_zone,
        utm_data_source,

        -- flags
        exclude_from_cost_calculations,

        -- comments
        comment,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
