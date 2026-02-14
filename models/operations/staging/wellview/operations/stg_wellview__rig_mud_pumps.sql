{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIGPUMP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as rig_pump_id,
        trim(idrecparent)::varchar as job_rig_id,
        trim(idwell)::varchar as well_id,

        -- pump identification
        trim(des)::varchar as pump_number,
        trim(refid)::varchar as reference_id,

        -- manufacturer information
        trim(make)::varchar as pump_manufacturer,
        trim(model)::varchar as pump_model,
        trim(sn)::varchar as serial_number,

        -- pump classification
        trim(actioncategory)::varchar as action_category,
        trim(actiontyp)::varchar as action_type,

        -- physical specifications (converted from meters to inches)
        {{ wv_meters_to_inches('strokelength') }} as stroke_length_in,
        {{ wv_meters_to_inches('szodrod') }} as rod_diameter_in,

        -- power rating (converted from watts to horsepower)
        {{ wv_watts_to_hp('powerrating') }} as power_rating_hp,

        -- date information
        dttmstart::timestamp_ntz as pump_start_datetime,
        dttmend::timestamp_ntz as pump_end_datetime,
        dttmmanufacture::timestamp_ntz as manufacture_datetime,

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
        and rig_pump_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['rig_pump_id']) }} as rig_mud_pump_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rig_mud_pump_sk,

        -- identifiers
        rig_pump_id,
        job_rig_id,
        well_id,

        -- pump identification
        pump_number,
        reference_id,

        -- manufacturer information
        pump_manufacturer,
        pump_model,
        serial_number,

        -- pump classification
        action_category,
        action_type,

        -- physical specifications
        stroke_length_in,
        rod_diameter_in,

        -- power rating
        power_rating_hp,

        -- date information
        pump_start_datetime,
        pump_end_datetime,
        manufacture_datetime,

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
