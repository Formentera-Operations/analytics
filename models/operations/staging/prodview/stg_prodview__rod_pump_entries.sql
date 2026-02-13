{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMPRODENTRY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent,
        trim(idflownet)::varchar as id_flownet,

        -- dates
        dttm::timestamp_ntz as observation_date,

        -- operational measurements
        -- Peloton conversion: SPM requires no conversion
        spm::float as spm,
        {{ pv_meters_to_inches('strokelength') }}::float as stroke_length_in,
        {{ pv_decimal_to_pct('usernum1') }}::float as run_time_pct,
        {{ pv_cbm_to_bbl_per_day('volperdaycalc') }}::float as vol_per_day_calc_bbl,

        -- comments and user fields
        trim(com)::varchar as comments,
        trim(usertxt1)::varchar as crank_position,
        trim(usertxt2)::varchar as crank_rotation,
        trim(usertxt3)::varchar as user_txt3,
        usernum2::float as user_num2,
        usernum3::float as user_num3,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,
        trim(systag)::varchar as record_tag,

        -- fivetran metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and id_rec is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as rod_pump_entry_sk,
        *,
        -- flag seed records (system placeholders with no operational data)
        coalesce(lower(trim(comments)) = 'seed record', false) as is_seed_record,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        rod_pump_entry_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        observation_date,

        -- operational measurements (Peloton unit conversions applied)
        spm,
        stroke_length_in,
        run_time_pct,
        vol_per_day_calc_bbl,

        -- comments and user fields
        comments,
        crank_position,
        crank_rotation,
        user_txt3,
        user_num2,
        user_num3,

        -- flags
        is_seed_record,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,
        record_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
