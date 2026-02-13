{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUID') }}
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
        trim(idrecunitnodecalc)::varchar as unit_node_id,
        trim(idrecunitnodecalctk)::varchar as unit_node_table,
        trim(idrecunitdataentryor)::varchar as data_entry_unit_id,
        trim(idrecunitdataentryortk)::varchar as data_entry_unit_table,

        -- meter configuration
        trim(name)::varchar as meter_name,
        trim(entrysource)::varchar as entry_source,
        trim(typ)::varchar as meter_type,
        trim(typrecording)::varchar as recording_type,
        trim(uomvol)::varchar as volume_unit_of_measure,
        trim(productname)::varchar as product_name,
        rezerostart::float as rezero_start_value,
        readingrollover::float as reading_rollover_value,
        trim(estmissingday)::varchar as estimate_missing_days,
        {{ pv_decimal_to_pct('initialbsw') }}::float as initial_bsw_pct,
        trim(serialnum)::varchar as serial_number,
        trim(engineeringid)::varchar as engineering_id,
        trim(regulatoryid)::varchar as regulatory_id,
        trim(otherid)::varchar as other_id,
        trim(locprovtap)::varchar as proving_tap_location,
        trim(entryreqperiod)::varchar as entry_requirement_period,

        -- dates
        dttmhide::timestamp_ntz as hide_record_date,

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
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,

        -- system / audit
        sysseq::float as sequence_number,
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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as liquid_meter_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        liquid_meter_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        unit_node_id,
        unit_node_table,
        data_entry_unit_id,
        data_entry_unit_table,

        -- meter configuration
        meter_name,
        entry_source,
        meter_type,
        recording_type,
        volume_unit_of_measure,
        product_name,
        rezero_start_value,
        reading_rollover_value,
        estimate_missing_days,
        initial_bsw_pct,
        serial_number,
        engineering_id,
        regulatory_id,
        other_id,
        proving_tap_location,
        entry_requirement_period,

        -- dates
        hide_record_date,

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
        user_text_1,
        user_text_2,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,

        -- system / audit
        sequence_number,
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
