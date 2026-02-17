{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'wellbore_surveys']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREDIRSURVEYDATA') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as survey_data_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as directional_survey_id,

        -- survey point measurements
        {{ wv_meters_to_feet('md') }} as measured_depth_ft,
        inclination::float as inclination_degrees,
        azimuth::float as azimuth_degrees,
        dttm::timestamp_ntz as survey_date,

        -- survey metadata
        trim(surveymethod)::varchar as survey_method,
        trim(surveyedby)::varchar as surveyed_by_company,

        -- calculated position coordinates (converted from meters to feet)
        {{ wv_meters_to_feet('tvdcalc') }} as tvd_ft,
        {{ wv_meters_to_feet('vscalc') }} as vs_ft,
        {{ wv_meters_to_feet('departcalc') }} as departure_ft,
        {{ wv_meters_to_feet('nscalc') }} as north_south_ft,
        {{ wv_meters_to_feet('ewcalc') }} as east_west_ft,
        {{ wv_meters_to_feet('tvdsscalc') }} as tvd_subsea_ft,
        {{ wv_meters_to_feet('displaceunwrapcalc') }} as unwrapped_displacement_ft,

        -- trajectory calculations (converted from deg/m to deg/100ft)
        {{ wv_per_m_to_per_100ft('dlscalc') }} as dogleg_severity_deg_per_100ft,
        {{ wv_per_m_to_per_100ft('buildratecalc') }} as build_rate_deg_per_100ft,
        {{ wv_per_m_to_per_100ft('turnratecalc') }} as turn_rate_deg_per_100ft,

        -- override values (converted from meters to feet)
        calcoverride::boolean as calculation_override_flag,
        {{ wv_meters_to_feet('tvdoverride') }} as tvd_override_ft,
        {{ wv_meters_to_feet('nsoverride') }} as north_south_override_ft,
        {{ wv_meters_to_feet('ewoverride') }} as east_west_override_ft,
        {{ wv_per_m_to_per_100ft('dlsoverride') }} as dogleg_severity_override_deg_per_100ft,
        {{ wv_meters_to_feet('vsoverride') }} as vs_override_ft,

        -- data quality flags
        dontuse::boolean as is_bad_survey_data,
        trim(dontusereason)::varchar as bad_survey_data_reason,
        trim(annotation)::varchar as annotation,
        trim(note)::varchar as note,

        -- tool face orientations (degrees)
        tfograv::float as tool_face_orientation_gravity_degrees,
        tfomag::float as tool_face_orientation_magnetic_degrees,

        -- survey tool information
        trim(tooltyp1)::varchar as tool_type,
        trim(tooltyp2)::varchar as tool_sub_type,
        trim(source)::varchar as data_source,
        trim(correction)::varchar as correction_method,
        trim(model)::varchar as tool_model,

        -- raw instrument readings - gravity (m/s2 to ft/s2)
        {{ wv_meters_to_feet('gravaxialraw') }} as gravity_axial_raw_ft_per_s2,
        {{ wv_meters_to_feet('gravtran1raw') }} as gravity_transverse_1_raw_ft_per_s2,
        {{ wv_meters_to_feet('gravtran2raw') }} as gravity_transverse_2_raw_ft_per_s2,

        -- raw instrument readings - magnetic (tesla to nanotesla)
        magaxialraw / 1e-09 as magnetic_axial_raw_nanotesla,
        magtran1raw / 1e-09 as magnetic_transverse_1_raw_nanotesla,
        magtran2raw / 1e-09 as magnetic_transverse_2_raw_nanotesla,

        -- geographic coordinates
        latitude::float as latitude_degrees,
        longitude::float as longitude_degrees,
        trim(latlongsource)::varchar as lat_long_data_source,
        utmx::float as utm_easting_meters,
        utmy::float as utm_northing_meters,
        utmgridzone::int as utm_grid_zone,
        trim(utmsource)::varchar as utm_data_source,

        -- system locking
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
        and survey_data_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['survey_data_id']) }} as survey_data_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        survey_data_sk,

        -- identifiers
        survey_data_id,
        well_id,
        directional_survey_id,

        -- survey point measurements
        measured_depth_ft,
        inclination_degrees,
        azimuth_degrees,
        survey_date,

        -- survey metadata
        survey_method,
        surveyed_by_company,

        -- calculated position coordinates
        tvd_ft,
        vs_ft,
        departure_ft,
        north_south_ft,
        east_west_ft,
        tvd_subsea_ft,
        unwrapped_displacement_ft,

        -- trajectory calculations
        dogleg_severity_deg_per_100ft,
        build_rate_deg_per_100ft,
        turn_rate_deg_per_100ft,

        -- override values
        calculation_override_flag,
        tvd_override_ft,
        north_south_override_ft,
        east_west_override_ft,
        dogleg_severity_override_deg_per_100ft,
        vs_override_ft,

        -- data quality flags
        is_bad_survey_data,
        bad_survey_data_reason,
        annotation,
        note,

        -- tool face orientations
        tool_face_orientation_gravity_degrees,
        tool_face_orientation_magnetic_degrees,

        -- survey tool information
        tool_type,
        tool_sub_type,
        data_source,
        correction_method,
        tool_model,

        -- raw instrument readings - gravity
        gravity_axial_raw_ft_per_s2,
        gravity_transverse_1_raw_ft_per_s2,
        gravity_transverse_2_raw_ft_per_s2,

        -- raw instrument readings - magnetic
        magnetic_axial_raw_nanotesla,
        magnetic_transverse_1_raw_nanotesla,
        magnetic_transverse_2_raw_nanotesla,

        -- geographic coordinates
        latitude_degrees,
        longitude_degrees,
        lat_long_data_source,
        utm_easting_meters,
        utm_northing_meters,
        utm_grid_zone,
        utm_data_source,

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
