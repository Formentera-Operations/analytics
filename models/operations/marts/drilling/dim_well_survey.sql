{{
    config(
        materialized='table',
        tags=['drilling', 'mart', 'dimension']
    )
}}

with surveys as (
    select * from {{ ref('stg_wellview__wellbore_directional_surveys') }}
),

survey_stations as (
    select * from {{ ref('stg_wellview__wellbore_directional_survey_data') }}
),

well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

wellbores as (
    select
        wellbore_id,
        wellbore_sk,
        profile_category
    from {{ ref('dim_wellbore') }}
),

joined as (
    select
        ss.survey_data_sk as well_survey_sk,

        -- dimensional FKs
        wb.wellbore_sk,
        w360.eid,

        -- natural keys
        ss.survey_data_id,
        ss.directional_survey_id as survey_id,
        s.survey_id as survey_header_id,
        ss.well_id,
        s.wellbore_id,
        s.job_id,

        -- survey identity
        s.proposed_or_actual,
        s.proposed_version_number,
        s.description as survey_description,
        s.use_for_calculations,
        s.is_definitive,
        s.azimuth_north_type,
        s.declination_degrees,
        s.convergence_degrees,

        -- station-level measurements
        ss.measured_depth_ft,
        ss.inclination_degrees,
        ss.azimuth_degrees,
        ss.survey_date as station_survey_date,
        ss.tvd_ft,
        ss.vs_ft,
        ss.departure_ft,
        ss.north_south_ft,
        ss.east_west_ft,
        ss.unwrapped_displacement_ft,
        ss.dogleg_severity_deg_per_100ft,
        ss.build_rate_deg_per_100ft,
        ss.turn_rate_deg_per_100ft,

        -- quality and context
        ss.is_bad_survey_data,
        ss.bad_survey_data_reason,
        ss.annotation,
        ss.survey_method,
        ss.surveyed_by_company,
        ss.tool_type,
        ss.tool_sub_type,
        ss.data_source,
        ss.correction_method,
        wb.profile_category as wellbore_profile_category,

        -- derived values
        coalesce(ss.survey_date, s.survey_date) as survey_date,

        -- derived flags
        row_number() over (
            partition by ss.directional_survey_id
            order by ss.measured_depth_ft desc nulls last, ss.survey_data_id desc
        ) = 1 as is_latest_station_in_survey,

        -- audit
        current_timestamp() as _loaded_at

    from survey_stations as ss
    left join surveys as s
        on ss.directional_survey_id = s.survey_id
    left join well_360 as w360
        on ss.well_id = w360.wellview_id
    left join wellbores as wb
        on s.wellbore_id = wb.wellbore_id
),

final as (
    select
        well_survey_sk,
        wellbore_sk,
        eid,
        survey_data_id,
        survey_id,
        survey_header_id,
        well_id,
        wellbore_id,
        job_id,
        proposed_or_actual,
        proposed_version_number,
        survey_description,
        use_for_calculations,
        is_definitive,
        azimuth_north_type,
        declination_degrees,
        convergence_degrees,
        survey_date,
        measured_depth_ft,
        inclination_degrees,
        azimuth_degrees,
        tvd_ft,
        vs_ft,
        departure_ft,
        north_south_ft,
        east_west_ft,
        unwrapped_displacement_ft,
        dogleg_severity_deg_per_100ft,
        build_rate_deg_per_100ft,
        turn_rate_deg_per_100ft,
        is_bad_survey_data,
        bad_survey_data_reason,
        annotation,
        survey_method,
        surveyed_by_company,
        tool_type,
        tool_sub_type,
        data_source,
        correction_method,
        wellbore_profile_category,
        is_latest_station_in_survey,
        _loaded_at
    from joined
)

select * from final
