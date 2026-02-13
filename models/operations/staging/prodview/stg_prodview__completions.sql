{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMP') }}
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

        -- completion lifecycle dates
        dttmstartalloc::timestamp_ntz as start_allocating_date,
        dttmend::timestamp_ntz as expiry_date,
        dttmonprod::timestamp_ntz as pop_date,
        dttmfirstsale::timestamp_ntz as first_sale_date,
        dttmflowbackstart::timestamp_ntz as flowback_start_date,
        dttmflowbackend::timestamp_ntz as flowback_end_date,
        dttmabandon::timestamp_ntz as abandon_date,
        dttmlastproducedcalc::timestamp_ntz as last_produced_date,
        dttmlastproducedhcliqcalc::timestamp_ntz as last_produced_oil_date,
        dttmlastproducedgascalc::timestamp_ntz as last_produced_gas_date,

        -- production threshold
        heldbyproductionthreshold::float as held_by_production_threshold,

        -- completion identifiers
        trim(completionname)::varchar as completion_name,
        trim(permanentid)::varchar as permanent_completion_id,
        trim(compidregulatory)::varchar as ghg_report_basin,
        trim(compidpa)::varchar as completion_prod_acct_id,
        trim(completionlicensee)::varchar as completion_licensee,
        trim(completionlicenseno)::varchar as federal_lease_number,
        dttmlicense::timestamp_ntz as completion_license_date,
        trim(compida)::varchar as well_number,
        trim(compidb)::varchar as gas_pop_id,
        trim(compidc)::varchar as gas_meter_number,
        trim(compidd)::varchar as gas_alloc_meter_number,
        trim(completionide)::varchar as gas_alloc_group_number,
        trim(completioncode)::varchar as surface_commingle_number,

        -- well identifiers
        trim(wellname)::varchar as well_name,
        trim(wellidregulatory)::varchar as well_regulatory_id,
        trim(wellidpa)::varchar as well_prod_acct_id,
        trim(welllicenseno)::varchar as well_license_number,
        trim(wellida)::varchar as api_10,
        trim(wellidb)::varchar as cost_center,
        trim(wellidc)::varchar as eid,
        trim(wellidd)::varchar as producing_formation,
        trim(wellide)::varchar as legal_well_name,

        -- import/export tracking
        trim(importid1)::varchar as import_id_1,
        trim(importtyp1)::varchar as import_type_1,
        trim(importid2)::varchar as import_id_2,
        trim(importtyp2)::varchar as import_type_2,
        trim(exportid1)::varchar as export_id_1,
        trim(exporttyp1)::varchar as export_type_1,
        trim(exportid2)::varchar as export_id_2,
        trim(exporttyp2)::varchar as export_type_2,

        -- location
        latitude::float as bottomhole_latitude,
        longitude::float as bottomhole_longitude,
        trim(latlongsource)::varchar as latlong_data_source,
        trim(latlongdatum)::varchar as latlong_datum,

        -- entry requirements
        trim(entryreqperiodfluidlevel)::varchar as entry_req_period_fluid_level,
        trim(entryreqperiodparam)::varchar as entry_req_period_param,

        -- user-defined fields - text
        trim(usertxt1)::varchar as bha_type,
        trim(usertxt2)::varchar as reserve_category,
        trim(usertxt3)::varchar as electric_vendor_name,
        trim(usertxt4)::varchar as electric_meter_name,
        trim(usertxt5)::varchar as working_interest_partner,

        -- user-defined fields - numeric
        usernum1::float as surface_casing,
        usernum2::float as prod_casing,
        usernum3::float as prod_liner,
        usernum4::float as purchaser_ctb_lease_id,
        usernum5::float as purchaser_well_lease_id,

        -- user-defined fields - datetime
        userdttm1::timestamp_ntz as spud_date,
        userdttm2::timestamp_ntz as user_date_2,
        userdttm3::timestamp_ntz as rig_release_date,
        userdttm4::timestamp_ntz as user_date_4,
        userdttm5::timestamp_ntz as user_date_5,

        -- migration tracking
        trim(keymigrationsource)::varchar as migration_source_key,
        trim(typmigrationsource)::varchar as migration_source_type,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as completion_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        completion_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- completion lifecycle dates
        start_allocating_date,
        expiry_date,
        pop_date,
        first_sale_date,
        flowback_start_date,
        flowback_end_date,
        abandon_date,
        last_produced_date,
        last_produced_oil_date,
        last_produced_gas_date,

        -- production threshold
        held_by_production_threshold,

        -- completion identifiers
        completion_name,
        permanent_completion_id,
        ghg_report_basin,
        completion_prod_acct_id,
        completion_licensee,
        federal_lease_number,
        completion_license_date,
        well_number,
        gas_pop_id,
        gas_meter_number,
        gas_alloc_meter_number,
        gas_alloc_group_number,
        surface_commingle_number,

        -- well identifiers
        well_name,
        well_regulatory_id,
        well_prod_acct_id,
        well_license_number,
        api_10,
        cost_center,
        eid,
        producing_formation,
        legal_well_name,

        -- import/export tracking
        import_id_1,
        import_type_1,
        import_id_2,
        import_type_2,
        export_id_1,
        export_type_1,
        export_id_2,
        export_type_2,

        -- location
        bottomhole_latitude,
        bottomhole_longitude,
        latlong_data_source,
        latlong_datum,

        -- entry requirements
        entry_req_period_fluid_level,
        entry_req_period_param,

        -- user-defined fields
        bha_type,
        reserve_category,
        electric_vendor_name,
        electric_meter_name,
        working_interest_partner,
        surface_casing,
        prod_casing,
        prod_liner,
        purchaser_ctb_lease_id,
        purchaser_well_lease_id,
        spud_date,
        user_date_2,
        rig_release_date,
        user_date_4,
        user_date_5,

        -- migration tracking
        migration_source_key,
        migration_source_type,

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
