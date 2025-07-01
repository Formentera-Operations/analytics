{{ config(
    materialized='view',
    tags=['wellview', 'wellbore', 'directional-surveys', 'survey-data', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBOREDIRSURVEYDATA') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as survey_data_id,
        idrecparent as directional_survey_id,
        idwell as well_id,
        
        -- Survey point measurements
        md / 0.3048 as measured_depth_ft,
        inclination as inclination_degrees,
        azimuth as azimuth_degrees,
        
        -- Survey metadata
        dttm as survey_datetime,
        surveymethod as survey_method,
        surveyedby as surveyed_by_company,
        
        -- Calculated position coordinates (converted to US units)
        tvdcalc / 0.3048 as tvd_ft,
        vscalc / 0.3048 as vs_ft,
        departcalc / 0.3048 as departure_ft,
        nscalc / 0.3048 as north_south_ft,
        ewcalc / 0.3048 as east_west_ft,
        
        -- Trajectory calculations (converted to US units)
        dlscalc / 0.0328083989501312 as dogleg_severity_deg_per_100ft,
        buildratecalc / 0.0328083989501312 as build_rate_deg_per_100ft,
        turnratecalc / 0.0328083989501312 as turn_rate_deg_per_100ft,
        displaceunwrapcalc / 0.3048 as unwrapped_displacement_ft,
        tvdsscalc / 0.3048 as tvd_subsea_ft,
        
        -- Override values (converted to US units)
        calcoverride as calculation_override_flag,
        tvdoverride / 0.3048 as tvd_override_ft,
        nsoverride / 0.3048 as north_south_override_ft,
        ewoverride / 0.3048 as east_west_override_ft,
        dlsoverride / 0.0328083989501312 as dogleg_severity_override_deg_per_100ft,
        vsoverride / 0.3048 as vs_override_ft,
        
        -- Data quality flags
        dontuse as exclude_from_calculations,
        dontusereason as exclude_reason,
        annotation as annotation,
        note as note,
        
        -- Raw instrument readings - Gravity (converted to US units)
        gravaxialraw / 0.3048 as gravity_axial_raw_ft_per_s2,
        gravtran1raw / 0.3048 as gravity_transverse_1_raw_ft_per_s2,
        gravtran2raw / 0.3048 as gravity_transverse_2_raw_ft_per_s2,
        
        -- Raw instrument readings - Magnetic (converted to nanoteslas)
        magaxialraw / 1E-09 as magnetic_axial_raw_nt,
        magtran1raw / 1E-09 as magnetic_transverse_1_raw_nt,
        magtran2raw / 1E-09 as magnetic_transverse_2_raw_nt,
        
        -- Tool face orientations (in degrees)
        tfograv as tool_face_orientation_gravity_degrees,
        tfomag as tool_face_orientation_magnetic_degrees,
        
        -- Survey tool information
        tooltyp1 as tool_type_1,
        tooltyp2 as tool_sub_type,
        source as data_source,
        correction as correction_method,
        model as tool_model,
        
        -- Geographic coordinates
        latitude as latitude_degrees,
        longitude as longitude_degrees,
        latlongsource as lat_long_data_source,
        
        -- UTM coordinates (kept in meters as per view definition)
        utmx as utm_easting_meters,
        utmy as utm_northing_meters,
        utmgridzone as utm_grid_zone,
        utmsource as utm_data_source,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
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