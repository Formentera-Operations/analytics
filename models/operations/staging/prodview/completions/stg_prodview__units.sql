{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNIT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idflownet)::varchar as id_flownet,
        trim(name)::varchar as unit_name,
        trim(nameshort)::varchar as unit_display_name,

        -- unit classification
        trim(typ1)::varchar as unit_type,
        trim(typ2)::varchar as unit_sub_type,
        trim(typregulatory)::varchar as regulatory_unit_type,
        trim(typdisphcliq)::varchar as hcliq_inventory_type,
        trim(typdispngl)::varchar as ngl_inventory_type,
        trim(dispproductname)::varchar as hcliq_disposition_method,
        trim(typpa)::varchar as commingle_permit_number,

        -- status and operational info
        priority::boolean as is_cycled,
        operated::boolean as is_operated,
        trim(operator)::varchar as operator_name,
        trim(operatorida)::varchar as operated_descriptor,
        purchaser::boolean as is_purchaser,
        displaysizefactor::float as display_size_factor,

        -- temporal information
        dttmstart::timestamp_ntz as start_display_date,
        dttmend::timestamp_ntz as stop_display_date,
        dttmhide::timestamp_ntz as hide_record_as_of,

        -- location - geographic coordinates
        latitude::float as surface_latitude,
        longitude::float as surface_longitude,
        trim(latlongsource)::varchar as latlong_data_source,
        trim(latlongdatum)::varchar as latlong_datum,

        -- location - utm coordinates
        trim(utmgridzone)::varchar as utm_grid_zone,
        utmx::float as utm_easting,
        utmy::float as utm_northing,
        trim(utmsource)::varchar as utm_source,

        -- location - physical/administrative
        {{ pv_meters_to_feet('elevation') }}::float as ground_elevation_ft,
        trim(legalsurfloc)::varchar as surface_legal_location,
        trim(division)::varchar as division,
        trim(divisioncode)::varchar as company_code,
        trim(district)::varchar as district,
        trim(area)::varchar as asset_company,
        trim(field)::varchar as foreman_area,
        trim(fieldcode)::varchar as regulatory_field_name,
        trim(fieldoffice)::varchar as field_office,
        trim(fieldofficecode)::varchar as district_office,
        trim(country)::varchar as country,
        trim(stateprov)::varchar as state_province,
        trim(county)::varchar as county,

        -- facility and infrastructure
        trim(platform)::varchar as route,
        trim(padcode)::varchar as facility_name,
        trim(padname)::varchar as pad_name,
        trim(slot)::varchar as dsu,
        trim(locationtyp)::varchar as swd_system,

        -- business identifiers
        trim(unitidregulatory)::varchar as regulatory_id,
        trim(unitidpa)::varchar as property_eid,
        trim(unitida)::varchar as api_10,
        trim(unitidb)::varchar as property_number,
        trim(unitidc)::varchar as combo_curve_id,
        trim(stopname)::varchar as stop_name,
        trim(lease)::varchar as lease_name,
        trim(leaseida)::varchar as lease_number,

        -- financial and organizational
        trim(costcenterida)::varchar as cost_center,
        trim(costcenteridb)::varchar as gas_gathering_system_name,
        trim(govauthority)::varchar as government_authority,

        -- current status references (calculated fields)
        trim(idrecroutesetroutecalc)::varchar as current_route_id,
        trim(idrecroutesetroutecalctk)::varchar as current_route_table,
        trim(idrecfacilitycalc)::varchar as current_facility_id,
        trim(idrecfacilitycalctk)::varchar as current_facility_table,
        trim(idreccompstatuscalc)::varchar as current_completion_status_id,
        trim(idreccompstatuscalctk)::varchar as current_completion_status_table,

        -- responsible parties
        trim(idrecresp1)::varchar as oil_purchaser_id,
        trim(idrecresp1tk)::varchar as oil_purchaser_table,
        trim(idrecresp2)::varchar as gas_purchaser_id,
        trim(idrecresp2tk)::varchar as gas_purchaser_table,

        -- migration tracking
        trim(keymigrationsource)::varchar as migration_source_key,
        trim(typmigrationsource)::varchar as migration_source_type,

        -- user-defined fields - text
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as completion_status,
        trim(usertxt3)::varchar as producing_method,
        trim(usertxt4)::varchar as stripper_type,
        trim(usertxt5)::varchar as chemical_provider,

        -- user-defined fields - numeric
        usernum1::float as electric_alloc_meter_number,
        usernum2::float as electric_meter_id,
        usernum3::float as electric_acct_number,
        usernum4::float as electric_vendor_number,
        usernum5::float as user_num_5,

        -- user-defined fields - datetime
        userdttm1::timestamp_ntz as stripper_date,
        userdttm2::timestamp_ntz as bha_change_1,
        userdttm3::timestamp_ntz as bha_change_2,
        userdttm4::timestamp_ntz as user_date_4,
        userdttm5::timestamp_ntz as user_date_5,

        -- administrative
        trim(sortbyuser)::varchar as unit_sort,
        trim(timezone)::varchar as timezone,
        trim(com)::varchar as comments,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as unit_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        unit_sk,

        -- identifiers
        id_rec,
        id_flownet,
        unit_name,
        unit_display_name,

        -- unit classification
        unit_type,
        unit_sub_type,
        regulatory_unit_type,
        hcliq_inventory_type,
        ngl_inventory_type,
        hcliq_disposition_method,
        commingle_permit_number,

        -- status and operational info
        is_cycled,
        is_operated,
        operator_name,
        operated_descriptor,
        is_purchaser,
        display_size_factor,

        -- temporal information
        start_display_date,
        stop_display_date,
        hide_record_as_of,

        -- location - geographic coordinates
        surface_latitude,
        surface_longitude,
        latlong_data_source,
        latlong_datum,

        -- location - utm coordinates
        utm_grid_zone,
        utm_easting,
        utm_northing,
        utm_source,

        -- location - physical/administrative
        ground_elevation_ft,
        surface_legal_location,
        division,
        company_code,
        district,
        asset_company,
        foreman_area,
        regulatory_field_name,
        field_office,
        district_office,
        country,
        state_province,
        county,

        -- facility and infrastructure
        route,
        facility_name,
        pad_name,
        dsu,
        swd_system,

        -- business identifiers
        regulatory_id,
        property_eid,
        api_10,
        property_number,
        combo_curve_id,
        stop_name,
        lease_name,
        lease_number,

        -- financial and organizational
        cost_center,
        gas_gathering_system_name,
        government_authority,

        -- current status references
        current_route_id,
        current_route_table,
        current_facility_id,
        current_facility_table,
        current_completion_status_id,
        current_completion_status_table,

        -- responsible parties
        oil_purchaser_id,
        oil_purchaser_table,
        gas_purchaser_id,
        gas_purchaser_table,

        -- migration tracking
        migration_source_key,
        migration_source_type,

        -- user-defined fields
        user_txt1,
        completion_status,
        producing_method,
        stripper_type,
        chemical_provider,
        electric_alloc_meter_number,
        electric_meter_id,
        electric_acct_number,
        electric_vendor_number,
        user_num_5,
        stripper_date,
        bha_change_1,
        bha_change_2,
        user_date_4,
        user_date_5,

        -- administrative
        unit_sort,
        timezone,
        comments,

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
