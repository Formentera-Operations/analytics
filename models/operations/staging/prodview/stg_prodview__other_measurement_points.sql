{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITMEASPT') }}
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

        -- measurement point information
        trim(name)::varchar as measurement_name,
        trim(idrecloc)::varchar as measurement_location_id,
        trim(idrecloctk)::varchar as measurement_location_table,
        trim(refida)::varchar as reference_id,
        trim(uomreading)::varchar as reading_unit_of_measure,
        trim(entryreqperiod)::varchar as entry_requirement_period,
        trim(com)::varchar as comment,

        -- import/export configuration
        trim(importid1)::varchar as import_id_1,
        trim(importtyp1)::varchar as import_type_1,
        trim(importid2)::varchar as import_id_2,
        trim(importtyp2)::varchar as import_type_2,
        trim(exportid1)::varchar as export_id_1,
        trim(exporttyp1)::varchar as export_type_1,
        trim(exportid2)::varchar as export_id_2,
        trim(exporttyp2)::varchar as export_type_2,

        -- grouping and organization
        trim(groupkey)::varchar as group_key,
        trim(groupname)::varchar as group_name,
        sysseq::float as sequence_number,

        -- migration tracking
        trim(keymigrationsource)::varchar as migration_source_key,
        trim(typmigrationsource)::varchar as migration_source_type,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        usernum1::float as user_num1,
        usernum2::float as user_num2,
        usernum3::float as user_num3,

        -- dates
        dttmhide::timestamp_ntz as hide_record_as_of,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as other_measurement_point_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        other_measurement_point_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- measurement point information
        measurement_name,
        measurement_location_id,
        measurement_location_table,
        reference_id,
        reading_unit_of_measure,
        entry_requirement_period,
        comment,

        -- import/export configuration
        import_id_1,
        import_type_1,
        import_id_2,
        import_type_2,
        export_id_1,
        export_type_1,
        export_id_2,
        export_type_2,

        -- grouping and organization
        group_key,
        group_name,
        sequence_number,

        -- migration tracking
        migration_source_key,
        migration_source_type,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_num1,
        user_num2,
        user_num3,

        -- dates
        hide_record_as_of,

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
