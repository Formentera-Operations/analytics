{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'wellbore_surveys']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBORE') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as parent_wellbore_id,
        trim(idrecparenttk)::varchar as parent_wellbore_table_key,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,

        -- wellbore identification
        trim(des)::varchar as wellbore_name,
        trim(wellboreida)::varchar as wellbore_api_uwi,
        trim(wellboreidb)::varchar as wellbore_id_b,
        trim(wellboreidc)::varchar as wellbore_id_c,
        trim(wellboreidd)::varchar as wellbore_id_d,
        trim(wellboreide)::varchar as wellbore_id_e,

        -- wellbore characteristics
        trim(purpose)::varchar as purpose,
        trim(profiletyp)::varchar as profile_type,
        trim(currentstatuscalc)::varchar as current_status,
        dttmstatuscalc::timestamp_ntz as current_status_date,

        -- depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthstart') }} as start_depth_ft,
        {{ wv_meters_to_feet('depthtvdstartcalc') }} as start_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtopactualcalc') }} as actual_top_depth_ft,
        {{ wv_meters_to_feet('depthtoppropcalc') }} as proposed_top_depth_ft,
        {{ wv_meters_to_feet('totaldepthcalc') }} as total_depth_ft,
        {{ wv_meters_to_feet('totaldepthtvdcalc') }} as total_depth_tvd_ft,
        {{ wv_meters_to_feet('depthdraw') }} as draw_depth_ft,
        {{ wv_meters_to_feet('depthkickoffmincalc') }} as min_kickoff_depth_ft,
        {{ wv_meters_to_feet('depthtvdkickoffmincalc') }} as min_kickoff_depth_tvd_ft,

        -- drilling dates
        dttmstartcalc::timestamp_ntz as start_drill_date,
        dttmendcalc::timestamp_ntz as end_drill_date,
        dttmkickoffmincalc::timestamp_ntz as min_kickoff_date,

        -- drilling duration (converted from days to hours)
        {{ wv_days_to_hours('durationcalc') }} as duration_hours,

        -- directional information
        vsdir::float as vertical_section_direction_degrees,
        {{ wv_meters_to_feet('vsoriginns') }} as vertical_section_origin_ns_ft,
        {{ wv_meters_to_feet('vsoriginew') }} as vertical_section_origin_ew_ft,
        closedircalc::float as closure_direction_degrees,
        inclmaxcalc::float as max_inclination_degrees,
        {{ wv_per_m_to_per_100ft('dlsmaxcalc') }} as max_dls_degrees_per_100ft,

        -- directional calculated values (converted from meters to feet)
        {{ wv_meters_to_feet('tvdmincalc') }} as min_tvd_ft,
        {{ wv_meters_to_feet('tvdmaxcalc') }} as max_tvd_ft,
        {{ wv_meters_to_feet('vsplotmincalc') }} as min_vs_for_wellbore_ft,
        {{ wv_meters_to_feet('vsplotmaxcalc') }} as max_vs_for_wellbore_ft,
        {{ wv_meters_to_feet('departmaxcalc') }} as max_departure_ft,
        {{ wv_meters_to_feet('nsmincalc') }} as min_ns_for_wellbore_ft,
        {{ wv_meters_to_feet('nsmaxcalc') }} as max_ns_for_wellbore_ft,
        {{ wv_meters_to_feet('ewmincalc') }} as min_ew_for_wellbore_ft,
        {{ wv_meters_to_feet('ewmaxcalc') }} as max_ew_for_wellbore_ft,
        {{ wv_meters_to_feet('displaceunwrapcalc') }} as unwrapped_displacement_ft,

        -- sensor durations (converted from days to minutes)
        {{ wv_days_to_minutes('duronbtmcalc') }} as duration_on_bottom_minutes,
        {{ wv_days_to_minutes('duroffbtmcalc') }} as duration_off_bottom_minutes,
        {{ wv_days_to_minutes('durpipemovingcalc') }} as duration_pipe_moving_minutes,

        -- bottom hole location
        trim(legalsurveytyp)::varchar as legal_survey_type,
        trim(legalsurveysubtyp)::varchar as legal_survey_subtype,
        trim(locationnote)::varchar as location_note,
        trim(legalsurveyloc)::varchar as bottom_hole_legal_location,
        {{ wv_meters_to_feet('nsdist') }} as north_south_distance_ft,
        trim(nsflag)::varchar as north_south_flag,
        {{ wv_meters_to_feet('ewdist') }} as east_west_distance_ft,
        trim(ewflag)::varchar as east_west_flag,
        trim(locationref)::varchar as location_reference,
        trim(county)::varchar as county,
        trim(stateprov)::varchar as state_province,
        trim(fieldname)::varchar as field_name,
        trim(fieldcode)::varchar as field_code,

        -- nearest town information
        trim(townname)::varchar as nearest_town,
        {{ wv_meters_to_miles('towndist') }} as distance_to_nearest_town_miles,
        trim(townflag)::varchar as nearest_town_ref_direction,
        trim(townstateprov)::varchar as nearest_town_state_prov,

        -- geographic coordinates
        trim(latlongsource)::varchar as lat_long_data_source,
        trim(latlongdatum)::varchar as lat_long_datum,
        latitude::float as latitude_degrees,
        longitude::float as longitude_degrees,
        trim(utmsource)::varchar as utm_source,
        trim(utmgridzone)::varchar as utm_grid_zone,
        utmx::float as utm_easting_meters,
        utmy::float as utm_northing_meters,

        -- configuration
        exclude::boolean as exclude_from_calculations,

        -- user fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        usernum1::float as user_number_1,
        usernum2::float as user_number_2,
        usernum3::float as user_number_3,

        -- comments
        trim(com)::varchar as comment,

        -- sequence
        sysseq::int as sequence_number,

        -- system locking fields
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as system_tag,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and record_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as wellbore_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        wellbore_sk,

        -- identifiers
        record_id,
        well_id,
        parent_wellbore_id,
        parent_wellbore_table_key,
        job_id,
        job_table_key,

        -- wellbore identification
        wellbore_name,
        wellbore_api_uwi,
        wellbore_id_b,
        wellbore_id_c,
        wellbore_id_d,
        wellbore_id_e,

        -- wellbore characteristics
        purpose,
        profile_type,
        current_status,
        current_status_date,

        -- depths
        start_depth_ft,
        start_depth_tvd_ft,
        actual_top_depth_ft,
        proposed_top_depth_ft,
        total_depth_ft,
        total_depth_tvd_ft,
        draw_depth_ft,
        min_kickoff_depth_ft,
        min_kickoff_depth_tvd_ft,

        -- dates
        start_drill_date,
        end_drill_date,
        min_kickoff_date,

        -- durations
        duration_hours,

        -- directional information
        vertical_section_direction_degrees,
        vertical_section_origin_ns_ft,
        vertical_section_origin_ew_ft,
        closure_direction_degrees,
        max_inclination_degrees,
        max_dls_degrees_per_100ft,

        -- directional calculated values
        min_tvd_ft,
        max_tvd_ft,
        min_vs_for_wellbore_ft,
        max_vs_for_wellbore_ft,
        max_departure_ft,
        min_ns_for_wellbore_ft,
        max_ns_for_wellbore_ft,
        min_ew_for_wellbore_ft,
        max_ew_for_wellbore_ft,
        unwrapped_displacement_ft,

        -- sensor durations
        duration_on_bottom_minutes,
        duration_off_bottom_minutes,
        duration_pipe_moving_minutes,

        -- bottom hole location
        legal_survey_type,
        legal_survey_subtype,
        location_note,
        bottom_hole_legal_location,
        north_south_distance_ft,
        north_south_flag,
        east_west_distance_ft,
        east_west_flag,
        location_reference,
        county,
        state_province,
        field_name,
        field_code,

        -- nearest town information
        nearest_town,
        distance_to_nearest_town_miles,
        nearest_town_ref_direction,
        nearest_town_state_prov,

        -- geographic coordinates
        lat_long_data_source,
        lat_long_datum,
        latitude_degrees,
        longitude_degrees,
        utm_source,
        utm_grid_zone,
        utm_easting_meters,
        utm_northing_meters,

        -- configuration
        exclude_from_calculations,

        -- user fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_number_1,
        user_number_2,
        user_number_3,

        -- comments
        comment,

        -- sequence
        sequence_number,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
