{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'general']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idwell (one row per well)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEADER') }}
    qualify 1 = row_number() over (
        partition by idwell
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idwell)::varchar as well_id,
        trim(wellname)::varchar as well_name,
        trim(wellida)::varchar as api_10_number,
        trim(wellidb)::varchar as cost_center,
        trim(wellidc)::varchar as eid,
        trim(wellidd)::varchar as producing_formation,
        trim(wellide)::varchar as producing_method,
        trim(lease)::varchar as lease_name,
        trim(leasecode)::varchar as state_lease_id,
        trim(padname)::varchar as pad_name,
        trim(padcode)::varchar as facility_name,

        -- well license
        trim(operator)::varchar as operator_name,
        trim(operatorcode)::varchar as operated_descriptor,
        trim(welllicenseno)::varchar as unique_well_identifier,
        trim(welllicensee)::varchar as licensee,
        trim(govauthority)::varchar as government_authority,

        -- well classification
        trim(basin)::varchar as basin_name,
        trim(basincode)::varchar as basin_code,
        trim(fieldname)::varchar as field_name,
        trim(fieldcode)::varchar as regulatory_field_name,
        trim(welltyp1)::varchar as well_type,
        trim(welltyp2)::varchar as well_subtype,
        trim(currentwelltyp1calc)::varchar as current_well_type,
        trim(currentwelltyp2calc)::varchar as current_well_subtype,
        trim(primaryfluiddes)::varchar as fluid_type,
        trim(currentprimaryfluiddescalc)::varchar as current_fluid_type,
        trim(wellconfig)::varchar as well_configuration_type,
        wellborenocalc::float as number_of_wellbores,
        trim(currentwellstatus1)::varchar as well_status,
        trim(currentwellstatus2)::varchar as well_sub_status,
        trim(currentwellstatus1calc)::varchar as current_well_status,
        trim(currentwellstatus2calc)::varchar as current_well_sub_status,
        dttmstatuscalc::timestamp_ntz as current_status_date,
        trim(wellclass)::varchar as sour_class,
        environmentsensitive::boolean as is_environment_sensitive,
        hih2s::boolean as is_high_h2s,
        hisitp::boolean as is_high_sitp,
        locationsensitive::boolean as is_location_sensitive,
        trim(riskclass)::varchar as risk_class,

        -- elevations (converted from metric to US units)
        {{ wv_meters_to_feet('elvorigkb') }} as original_kb_elevation_ft,
        {{ wv_meters_to_feet('elvground') }} as ground_elevation_ft,
        {{ wv_meters_to_feet('elvcasflange') }} as casing_flange_elevation_ft,
        {{ wv_meters_to_feet('elvtubhead') }} as tubing_head_elevation_ft,
        {{ wv_meters_to_feet('elvmudline') }} as mud_line_elevation_ft,
        {{ wv_meters_to_feet('elvlat') }} as lowest_astronomical_tide_elevation_ft,
        trim(idrecelvhistorycalc)::varchar as active_working_elevation_id,
        trim(idrecelvhistorycalctk)::varchar as active_working_elevation_table_key,

        -- elevation differences (converted from metric to US units)
        {{ wv_meters_to_feet('kbtotubcalc') }} as kb_to_tubing_head_distance_ft,
        {{ wv_meters_to_feet('kbtocascalc') }} as kb_to_casing_flange_distance_ft,
        {{ wv_meters_to_feet('kbtogrdcalc') }} as kb_to_ground_distance_ft,
        {{ wv_meters_to_feet('kbtomudcalc') }} as kb_to_mud_line_distance_ft,
        {{ wv_meters_to_feet('kbtoothercalc') }} as kb_to_other_distance_ft,
        {{ wv_meters_to_feet('othertotubcalc') }} as other_to_tubing_head_distance_ft,
        {{ wv_meters_to_feet('othertocascalc') }} as other_to_casing_flange_distance_ft,
        {{ wv_meters_to_feet('othertogrdcalc') }} as other_to_ground_distance_ft,
        {{ wv_meters_to_feet('othertomudcalc') }} as other_to_mud_line_distance_ft,

        -- depths (converted from metric to US units)
        trim(waterdepthref)::varchar as water_depth_reference,
        {{ wv_meters_to_feet('waterdepth') }} as water_depth_ft,
        {{ wv_meters_to_feet('tdcalc') }} as total_depth_ft,
        tdallcalc::float as total_depth_all,
        tdtvdallcalc::float as total_depth_all_tvd,
        pbtdallcalc::float as pbtd_all,
        {{ wv_meters_to_feet('displaceunwrapcalc') }} as unwrapped_displacement_ft,

        -- dates
        dttmwelllic::timestamp_ntz as permit_date,
        dttmspud::timestamp_ntz as spud_date,
        dttmrr::timestamp_ntz as rig_release_date,
        dttmfirstprodest::timestamp_ntz as estimated_on_production_date,
        dttmfirstprod::timestamp_ntz as on_production_date,
        dttmzoneonprodfirstcalc::timestamp_ntz as first_zone_on_production_date,
        dttmlastprodest::timestamp_ntz as estimated_last_production_date,
        dttmlastprod::timestamp_ntz as last_production_date,
        dttmabandonest::timestamp_ntz as estimated_abandonment_date,
        dttmabandon::timestamp_ntz as abandon_date,
        dttmwellclass::timestamp_ntz as h2s_certificate_approval_date,
        durspudtotodaycalc::float as age_of_well_days,

        -- location
        trim(locationtyp)::varchar as onshore_offshore_designation,
        trim(legalsurveytyp)::varchar as legal_survey_type,
        trim(legalsurveysubtyp)::varchar as legal_survey_subtype,
        trim(locationnote)::varchar as joa_name,
        trim(legalsurveyloc)::varchar as surface_legal_location,
        {{ wv_meters_to_feet('nsdist') }} as north_south_distance_ft,
        trim(nsflag)::varchar as north_south_reference,
        {{ wv_meters_to_feet('ewdist') }} as east_west_distance_ft,
        trim(ewflag)::varchar as east_west_reference,
        trim(townname)::varchar as nearest_town,
        {{ wv_meters_to_miles('towndist') }} as distance_to_nearest_town_miles,
        trim(townflag)::varchar as nearest_town_ref_direction,
        trim(townstateprov)::varchar as nearest_town_state_prov,
        trim(locationref)::varchar as location_reference,
        trim(area)::varchar as asset_company,
        trim(county)::varchar as county_parish,
        trim(stateprov)::varchar as state_province,
        trim(country)::varchar as country,
        trim(fieldoffice)::varchar as field_office,
        trim(fieldofficecode)::varchar as district_office,
        trim(district)::varchar as district,
        trim(division)::varchar as division,
        trim(divisioncode)::varchar as company_code,
        trim(directionstowell)::varchar as directions_to_well,

        -- coordinates
        trim(latlongsource)::varchar as lat_long_data_source,
        trim(latlongdatum)::varchar as lat_long_datum,
        latitude::float as latitude_degrees,
        longitude::float as longitude_degrees,
        trim(utmsource)::varchar as utm_source,
        trim(utmgridzone)::varchar as utm_grid_zone,
        utmx::float as utm_easting_meters,
        utmy::float as utm_northing_meters,

        -- operational
        trim(platform)::varchar as route,
        trim(slot)::varchar as dsu,
        trim(idrecprodsettingcalc)::varchar as production_setting_id,
        trim(idrecprodsettingcalctk)::varchar as production_setting_table_key,
        casingruncalc::float as number_of_casing_strings,
        problemflag::boolean as has_problem_with_well,
        trim(idrecproblemcalc)::varchar as production_failure_id,
        trim(idrecproblemcalctk)::varchar as production_failure_table_key,
        problemtotalcalc::float as total_number_of_failures,
        problemlast12monthcalc::float as failures_in_last_12_months,
        operated::boolean as is_operated,
        trim(surfacerights)::varchar as surface_rights,
        trim(agent)::varchar as agent,
        {{ wv_days_to_hours('localtimezone') }} as local_time_zone_hours,
        trim(lastjobcalc)::varchar as last_job,
        trim(lastjobreportcalc)::varchar as last_daily_ops_report,

        -- user fields
        trim(usertxt1)::varchar as rtp_lease_clause,
        trim(usertxt2)::varchar as election_days,
        trim(usertxt3)::varchar as payout_penalty_percent,
        trim(usertxt4)::varchar as order_number,
        trim(usertxt5)::varchar as mawp_psig,
        trim(usertxt6)::varchar as mawp_weak_point_description,
        trim(usertxt7)::varchar as maasp_psig,
        trim(usertxt8)::varchar as uic_permit_pressure_psig,
        trim(usertxt9)::varchar as uic_permit_rate_bpd,
        trim(usertxt10)::varchar as acquisition_accounting_id,
        userboolean1::boolean as is_last_well_on_lease,
        userboolean2::boolean as is_blm,
        userboolean3::boolean as is_fed_surface,
        userboolean4::boolean as is_split_estate,
        userboolean5::boolean as is_fo_drilled,
        usernum1::float as working_interest,
        usernum2::float as nri_total,
        usernum3::float as nri_wi_only,
        usernum4::float as override_decimal,
        usernum5::float as mineral_royalty_decimal,
        usernum6::float as shut_in_clock_days,
        userdttm1::timestamp_ntz as first_sales_date,
        userdttm2::timestamp_ntz as ops_effective_date,
        userdttm3::timestamp_ntz as regulatory_effective_date,
        userdttm4::timestamp_ntz as last_approved_mit_date,
        userdttm5::timestamp_ntz as joa_date,

        -- comments
        trim(com)::varchar as land_legal_description,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,
        sysmoddatedb::timestamp_ntz as last_write_to_database,
        trim(sysmoduserdb)::varchar as last_write_to_database_user,
        trim(syssecuritytyp)::varchar as security_type,
        syslockdatemaster::timestamp_ntz as master_lock_date,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and well_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['well_id']) }} as well_header_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        well_header_sk,

        -- identifiers
        well_id,
        well_name,
        api_10_number,
        cost_center,
        eid,
        producing_formation,
        producing_method,
        lease_name,
        state_lease_id,
        pad_name,
        facility_name,

        -- well license
        operator_name,
        operated_descriptor,
        unique_well_identifier,
        licensee,
        government_authority,

        -- well classification
        basin_name,
        basin_code,
        field_name,
        regulatory_field_name,
        well_type,
        well_subtype,
        current_well_type,
        current_well_subtype,
        fluid_type,
        current_fluid_type,
        well_configuration_type,
        number_of_wellbores,
        well_status,
        well_sub_status,
        current_well_status,
        current_well_sub_status,
        current_status_date,
        sour_class,
        is_environment_sensitive,
        is_high_h2s,
        is_high_sitp,
        is_location_sensitive,
        risk_class,

        -- elevations
        original_kb_elevation_ft,
        ground_elevation_ft,
        casing_flange_elevation_ft,
        tubing_head_elevation_ft,
        mud_line_elevation_ft,
        lowest_astronomical_tide_elevation_ft,
        active_working_elevation_id,
        active_working_elevation_table_key,

        -- elevation differences
        kb_to_tubing_head_distance_ft,
        kb_to_casing_flange_distance_ft,
        kb_to_ground_distance_ft,
        kb_to_mud_line_distance_ft,
        kb_to_other_distance_ft,
        other_to_tubing_head_distance_ft,
        other_to_casing_flange_distance_ft,
        other_to_ground_distance_ft,
        other_to_mud_line_distance_ft,

        -- depths
        water_depth_reference,
        water_depth_ft,
        total_depth_ft,
        total_depth_all,
        total_depth_all_tvd,
        pbtd_all,
        unwrapped_displacement_ft,

        -- dates
        permit_date,
        spud_date,
        rig_release_date,
        estimated_on_production_date,
        on_production_date,
        first_zone_on_production_date,
        estimated_last_production_date,
        last_production_date,
        estimated_abandonment_date,
        abandon_date,
        h2s_certificate_approval_date,
        age_of_well_days,

        -- location
        onshore_offshore_designation,
        legal_survey_type,
        legal_survey_subtype,
        joa_name,
        surface_legal_location,
        north_south_distance_ft,
        north_south_reference,
        east_west_distance_ft,
        east_west_reference,
        nearest_town,
        distance_to_nearest_town_miles,
        nearest_town_ref_direction,
        nearest_town_state_prov,
        location_reference,
        asset_company,
        county_parish,
        state_province,
        country,
        field_office,
        district_office,
        district,
        division,
        company_code,
        directions_to_well,

        -- coordinates
        lat_long_data_source,
        lat_long_datum,
        latitude_degrees,
        longitude_degrees,
        utm_source,
        utm_grid_zone,
        utm_easting_meters,
        utm_northing_meters,

        -- operational
        route,
        dsu,
        production_setting_id,
        production_setting_table_key,
        number_of_casing_strings,
        has_problem_with_well,
        production_failure_id,
        production_failure_table_key,
        total_number_of_failures,
        failures_in_last_12_months,
        is_operated,
        surface_rights,
        agent,
        local_time_zone_hours,
        last_job,
        last_daily_ops_report,

        -- user fields
        rtp_lease_clause,
        election_days,
        payout_penalty_percent,
        order_number,
        mawp_psig,
        mawp_weak_point_description,
        maasp_psig,
        uic_permit_pressure_psig,
        uic_permit_rate_bpd,
        acquisition_accounting_id,
        is_last_well_on_lease,
        is_blm,
        is_fed_surface,
        is_split_estate,
        is_fo_drilled,
        working_interest,
        nri_total,
        nri_wi_only,
        override_decimal,
        mineral_royalty_decimal,
        shut_in_clock_days,
        first_sales_date,
        ops_effective_date,
        regulatory_effective_date,
        last_approved_mit_date,
        joa_date,

        -- comments
        land_legal_description,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
        system_tag,
        last_write_to_database,
        last_write_to_database_user,
        security_type,
        master_lock_date,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
