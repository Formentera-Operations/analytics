{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPDOWNTM') }}
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

        -- downtime classification
        trim(typdowntm)::varchar as downtime_type,
        trim(product)::varchar as product,
        trim(location)::varchar as location,
        failflag::boolean as is_failure,

        -- downtime period - start
        dttmstart::date as first_day,
        {{ pv_days_to_hours('durdownstartday') }}::float as hours_down,

        -- downtime period - end
        dttmend::date as last_day,
        {{ pv_days_to_hours('durdownendday') }}::float as downtime_last_day_hours,

        -- total downtime
        {{ pv_days_to_hours('durdowncalc') }}::float as total_downtime_hours,

        -- planned downtime
        dttmplanend::timestamp_ntz as planned_end_date,
        {{ pv_days_to_hours('durdownplanend') }}::float as planned_downtime_hours,

        -- downtime codes
        trim(codedowntm1)::varchar as downtime_code_1,
        trim(codedowntm2)::varchar as downtime_code_2,
        trim(codedowntm3)::varchar as downtime_code_3,

        -- comments
        trim(com)::varchar as comments,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        trim(usertxt4)::varchar as user_txt4,
        trim(usertxt5)::varchar as user_txt5,
        usernum1::float as user_num1,
        usernum2::float as user_num2,
        usernum3::float as user_num3,
        usernum4::float as user_num4,
        usernum5::float as user_num5,
        userdttm1::timestamp_ntz as user_date_1,
        userdttm2::timestamp_ntz as user_date_2,
        userdttm3::timestamp_ntz as user_date_3,
        userdttm4::timestamp_ntz as user_date_4,
        userdttm5::timestamp_ntz as user_date_5,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as completion_downtime_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        completion_downtime_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- downtime classification
        downtime_type,
        product,
        location,
        is_failure,

        -- downtime period - start
        first_day,
        hours_down,

        -- downtime period - end
        last_day,
        downtime_last_day_hours,

        -- total downtime
        total_downtime_hours,

        -- planned downtime
        planned_end_date,
        planned_downtime_hours,

        -- downtime codes
        downtime_code_1,
        downtime_code_2,
        downtime_code_3,

        -- comments
        comments,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_txt4,
        user_txt5,
        user_num1,
        user_num2,
        user_num3,
        user_num4,
        user_num5,
        user_date_1,
        user_date_2,
        user_date_3,
        user_date_4,
        user_date_5,

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
