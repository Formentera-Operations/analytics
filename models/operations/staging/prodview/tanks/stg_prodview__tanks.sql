{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITTANK') }}
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

        -- descriptive fields
        trim(name)::varchar as tank_name,
        trim(productname)::varchar as product,
        trim(facproductname)::varchar as facility_product,
        trim(methodmeasure)::varchar as measurement_method,
        trim(uomlevel)::varchar as level_reading_unit,
        trim(vrmethod)::varchar as vapor_recovery_method,
        trim(typrecording)::varchar as recording_type,
        trim(serialnum)::varchar as serial_number,
        trim(engineeringid)::varchar as engineering_id,
        trim(regulatoryid)::varchar as regulatory_id,
        trim(otherid)::varchar as other_id,
        trim(entryreqperiod)::varchar as entry_requirement_period,

        -- tank capacity and status (converted to US units)
        {{ pv_cbm_to_bbl('volcapacity') }}::float as tank_capacity_bbl,
        {{ pv_cbm_to_bbl('volremaincalc') }}::float as remaining_capacity_bbl,
        {{ pv_decimal_to_pct('pctfullcalc') }}::float as capacity_percent_full,
        {{ pv_decimal_to_pct('initialbsw') }}::float as initial_bsw_pct,

        -- operational flags
        excludefmprod::boolean as exclude_from_production,
        underground::boolean as underground_tank,

        -- dates
        dttmstart::timestamp_ntz as start_using_tank,
        dttmend::timestamp_ntz as stop_using_tank,
        dttmhide::timestamp_ntz as hide_record_as_of,

        -- data entry references
        trim(idrecunitdataentryor)::varchar as data_entry_unit_id,
        trim(idrecunitdataentryortk)::varchar as data_entry_unit_table,

        -- import/export configuration
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
        sysseq::float as system_sequence,

        -- user-defined fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as tank_sk,
        *,
        coalesce(stop_using_tank is null, false) as is_active,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tank_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- descriptive fields
        tank_name,
        product,
        facility_product,
        measurement_method,
        level_reading_unit,
        vapor_recovery_method,
        recording_type,
        serial_number,
        engineering_id,
        regulatory_id,
        other_id,
        entry_requirement_period,

        -- tank capacity and status
        tank_capacity_bbl,
        remaining_capacity_bbl,
        capacity_percent_full,
        initial_bsw_pct,

        -- operational flags
        exclude_from_production,
        underground_tank,
        is_active,

        -- dates
        start_using_tank,
        stop_using_tank,
        hide_record_as_of,

        -- data entry references
        data_entry_unit_id,
        data_entry_unit_table,

        -- import/export configuration
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
        system_sequence,

        -- user-defined fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,

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
