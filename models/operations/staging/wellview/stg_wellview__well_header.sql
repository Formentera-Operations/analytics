{{ config(
    materialized='view',
    tags=['wellview', 'well-header', 'master-data', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEADER') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifier
        idwell as well_id,
        
        -- Well identifiers
        wellname as well_name,
        wellida as api_10_number,
        wellidb as cost_center,
        wellidc as eid,
        wellidd as producing_formation,
        wellide as producing_method,
        lease as lease_name,
        leasecode as state_lease_id,
        padname as pad_name,
        padcode as facility_name,
        
        -- Well license information
        operator as operator_name,
        operatorcode as operated_descriptor,
        welllicenseno as unique_well_identifier,
        welllicensee as licensee,
        govauthority as government_authority,
        
        -- Well classification
        basin as basin_name,
        basincode as basin_code,
        fieldname as field_name,
        fieldcode as regulatory_field_name,
        welltyp1 as well_type,
        welltyp2 as well_subtype,
        currentwelltyp1calc as current_well_type,
        currentwelltyp2calc as current_well_subtype,
        primaryfluiddes as fluid_type,
        currentprimaryfluiddescalc as current_fluid_type,
        wellconfig as well_configuration_type,
        wellborenocalc as number_of_wellbores,
        currentwellstatus1 as well_status,
        currentwellstatus2 as well_sub_status,
        currentwellstatus1calc as current_well_status,
        currentwellstatus2calc as current_well_sub_status,
        dttmstatuscalc as current_status_date,
        wellclass as sour_class,
        environmentsensitive as is_environment_sensitive,
        hih2s as is_high_h2s,
        hisitp as is_high_sitp,
        locationsensitive as is_location_sensitive,
        riskclass as risk_class,
        
        -- Elevations (converted to US units)
        elvorigkb / 0.3048 as original_kb_elevation_ft,
        elvground / 0.3048 as ground_elevation_ft,
        elvcasflange / 0.3048 as casing_flange_elevation_ft,
        elvtubhead / 0.3048 as tubing_head_elevation_ft,
        elvmudline / 0.3048 as mud_line_elevation_ft,
        elvlat / 0.3048 as lowest_astronomical_tide_elevation_ft,
        idrecelvhistorycalc as active_working_elevation_id,
        idrecelvhistorycalctk as active_working_elevation_table_key,
        
        -- Elevation differences (converted to US units)
        kbtotubcalc / 0.3048 as kb_to_tubing_head_distance_ft,
        kbtocascalc / 0.3048 as kb_to_casing_flange_distance_ft,
        kbtogrdcalc / 0.3048 as kb_to_ground_distance_ft,
        kbtomudcalc / 0.3048 as kb_to_mud_line_distance_ft,
        kbtoothercalc / 0.3048 as kb_to_other_distance_ft,
        othertotubcalc / 0.3048 as other_to_tubing_head_distance_ft,
        othertocascalc / 0.3048 as other_to_casing_flange_distance_ft,
        othertogrdcalc / 0.3048 as other_to_ground_distance_ft,
        othertomudcalc / 0.3048 as other_to_mud_line_distance_ft,
        
        -- Depths (converted to US units)
        waterdepthref as water_depth_reference,
        waterdepth / 0.3048 as water_depth_ft,
        tdcalc / 0.3048 as total_depth_ft,
        tdallcalc as total_depth_all,
        tdtvdallcalc as total_depth_all_tvd,
        pbtdallcalc as pbtd_all,
        displaceunwrapcalc / 0.3048 as unwrapped_displacement_ft,
        
        -- Important dates
        dttmwelllic as permit_date,
        dttmspud as spud_date,
        dttmrr as rig_release_date,
        dttmfirstprodest as estimated_on_production_date,
        dttmfirstprod as on_production_date,
        dttmzoneonprodfirstcalc as first_zone_on_production_date,
        dttmlastprodest as estimated_last_production_date,
        dttmlastprod as last_production_date,
        dttmabandonest as estimated_abandonment_date,
        dttmabandon as abandon_date,
        dttmwellclass as h2s_certificate_approval_date,
        durspudtotodaycalc as age_of_well_days,
        
        -- Location information
        locationtyp as onshore_offshore_designation,
        legalsurveytyp as legal_survey_type,
        legalsurveysubtyp as legal_survey_subtype,
        locationnote as joa_name,
        legalsurveyloc as surface_legal_location,
        nsdist / 0.3048 as north_south_distance_ft,
        nsflag as north_south_reference,
        ewdist / 0.3048 as east_west_distance_ft,
        ewflag as east_west_reference,
        townname as nearest_town,
        towndist / 1609.344 as distance_to_nearest_town_miles,
        townflag as nearest_town_ref_direction,
        townstateprov as nearest_town_state_prov,
        locationref as location_reference,
        area as asset_company,
        county as county_parish,
        stateprov as state_province,
        country as country,
        fieldoffice as field_office,
        fieldofficecode as district_office,
        district as district,
        division as division,
        divisioncode as company_code,
        directionstowell as directions_to_well,
        
        -- Geographic coordinates
        latlongsource as lat_long_data_source,
        latlongdatum as lat_long_datum,
        latitude as latitude_degrees,
        longitude as longitude_degrees,
        utmsource as utm_source,
        utmgridzone as utm_grid_zone,
        utmx as utm_easting_meters,
        utmy as utm_northing_meters,
        
        -- Other operational information
        platform as route,
        slot as dsu,
        idrecprodsettingcalc as production_setting_id,
        idrecprodsettingcalctk as production_setting_table_key,
        casingruncalc as number_of_casing_strings,
        problemflag as has_problem_with_well,
        idrecproblemcalc as production_failure_id,
        idrecproblemcalctk as production_failure_table_key,
        problemtotalcalc as total_number_of_failures,
        problemlast12monthcalc as failures_in_last_12_months,
        operated as is_operated,
        surfacerights as surface_rights,
        agent as agent,
        localtimezone / 0.0416666666666667 as local_time_zone_hours,
        lastjobcalc as last_job,
        lastjobreportcalc as last_daily_ops_report,
        
        -- User fields
        usertxt1 as rtp_lease_clause,
        usertxt2 as election_days,
        usertxt3 as payout_penalty_percent,
        usertxt4 as order_number,
        usertxt5 as mawp_psig,
        usertxt6 as mawp_weak_point_description,
        usertxt7 as maasp_psig,
        usertxt8 as uic_permit_pressure_psig,
        usertxt9 as uic_permit_rate_bpd,
        usertxt10 as acquisition_accounting_id,
        userboolean1 as is_last_well_on_lease,
        userboolean2 as is_blm,
        userboolean3 as is_fed_surface,
        userboolean4 as is_split_estate,
        userboolean5 as is_fo_drilled,
        usernum1 as working_interest,
        usernum2 as nri_total,
        usernum3 as nri_wi_only,
        usernum4 as override_decimal,
        usernum5 as mineral_royalty_decimal,
        usernum6 as shut_in_clock_days,
        userdttm1 as first_sales_date,
        userdttm2 as ops_effective_date,
        userdttm3 as regulatory_effective_date,
        userdttm4 as last_approved_mit_date,
        userdttm5 as joa_date,
        
        -- Comments
        com as land_legal_description,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        sysmoddatedb as last_write_to_database,
        sysmoduserdb as last_write_to_database_user,
        syssecuritytyp as security_type,
        syslockdatemaster as master_lock_date,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        
        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed