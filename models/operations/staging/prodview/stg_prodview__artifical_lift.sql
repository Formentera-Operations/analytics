{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMP') }}
        qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent,
        trim(idflownet)::varchar as id_flownet,

        -- equipment details
        trim(name)::varchar as pump_name,
        trim(make)::varchar as pump_make,
        trim(model)::varchar as pump_model,
        size::float as pump_size,
        trim(material)::varchar as pump_material,
        trim(typ)::varchar as pump_type,
        {{ pv_watts_to_hp('powerrating') }}::float as power_rating_hp,
        trim(controller)::varchar as controller,
        trim(serialnum)::varchar as serial_number,
        trim(engineeringid)::varchar as engineering_id,
        trim(regulatoryid)::varchar as regulatory_id,
        trim(otherid)::varchar as other_id,
        trim(entryreqperiod)::varchar as entry_req_period,
        trim(com)::varchar as comments,

        -- dates
        dttmstart::timestamp_ntz as install_date,
        dttmend::timestamp_ntz as removal_date,
        dttmhide::timestamp_ntz as hide_record_as_of,

        -- calculated fields
        daysinholecalc::float as days_in_hole_calc,

        -- import/export identifiers
        trim(importid1)::varchar as import_id_1,
        trim(importtyp1)::varchar as import_type_1,
        trim(importid2)::varchar as import_id_2,
        trim(importtyp2)::varchar as import_type_2,
        trim(exportid1)::varchar as export_id_1,
        trim(exporttyp1)::varchar as export_type_1,
        trim(exportid2)::varchar as export_id_2,
        trim(exporttyp2)::varchar as export_type_2,

        -- migration tracking
        trim(keymigrationsource)::varchar as migration_source_key,
        trim(typmigrationsource)::varchar as migration_source_type,

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
    where coalesce(_fivetran_deleted, false) = false
      and id_rec is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as artificial_lift_sk,
        *,
        -- flag whether pump is currently installed (no removal date)
        case
            when removal_date is null then true
            else false
        end as is_active,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        artificial_lift_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- equipment details
        pump_name,
        pump_make,
        pump_model,
        pump_size,
        pump_material,
        pump_type,
        power_rating_hp,
        controller,
        serial_number,
        engineering_id,
        regulatory_id,
        other_id,
        entry_req_period,
        comments,

        -- dates
        install_date,
        removal_date,
        hide_record_as_of,

        -- calculated fields
        days_in_hole_calc,

        -- flags
        is_active,

        -- import/export identifiers
        import_id_1,
        import_type_1,
        import_id_2,
        import_type_2,
        export_id_1,
        export_type_1,
        export_id_2,
        export_type_2,

        -- migration tracking
        migration_source_key,
        migration_source_type,

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