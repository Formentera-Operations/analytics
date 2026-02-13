{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERORIFICE') }}
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
        trim(idrecduropsource)::varchar as operating_hours_source_id,
        trim(idrecduropsourcetk)::varchar as operating_hours_source_table,
        trim(idrecunitnodecalc)::varchar as node_id,
        trim(idrecunitnodecalctk)::varchar as node_table,
        trim(idrecunitdataentryor)::varchar as data_entry_unit_id,
        trim(idrecunitdataentryortk)::varchar as data_entry_unit_table,

        -- meter configuration
        trim(name)::varchar as meter_name,
        trim(entrysource)::varchar as entry_source,
        trim(typreading)::varchar as chart_reading_type,
        trim(typrecording)::varchar as recording_type,
        trim(uomgasstat)::varchar as static_pressure_units,
        trim(uomgasdiff)::varchar as differential_pressure_units,
        trim(uomtemp)::varchar as temperature_units,
        trim(uomszorifice)::varchar as orifice_size_units,
        {{ pv_meters_to_inches('szrun') }}::float as run_size_inches,
        trim(metaltypepipe)::varchar as pipe_material,
        trim(metaltypeplate)::varchar as plate_material,
        trim(chartperiod)::varchar as chart_period,
        trim(typtap)::varchar as tap_type,
        trim(locstatictap)::varchar as static_tap_location,
        trim(locprovtap)::varchar as proving_tap_location,
        trim(estmissingday)::varchar as estimate_missing_days,
        trim(serialnum)::varchar as serial_number,
        trim(engineeringid)::varchar as engineering_id,
        trim(regulatoryid)::varchar as regulatory_id,
        trim(otherid)::varchar as other_id,
        trim(entryreqperiod)::varchar as entry_requirement_period,

        -- dates
        dttmhide::timestamp_ntz as hide_record_as_of,

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
        sysseq::float as system_sequence,
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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as gas_meter_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        gas_meter_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        operating_hours_source_id,
        operating_hours_source_table,
        node_id,
        node_table,
        data_entry_unit_id,
        data_entry_unit_table,

        -- meter configuration
        meter_name,
        entry_source,
        chart_reading_type,
        recording_type,
        static_pressure_units,
        differential_pressure_units,
        temperature_units,
        orifice_size_units,
        run_size_inches,
        pipe_material,
        plate_material,
        chart_period,
        tap_type,
        static_tap_location,
        proving_tap_location,
        estimate_missing_days,
        serial_number,
        engineering_id,
        regulatory_id,
        other_id,
        entry_requirement_period,

        -- dates
        hide_record_as_of,

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
        system_sequence,
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
