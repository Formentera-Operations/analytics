{{ config(
    materialized='view',
    tags=['wellview', 'wellbore', 'directional', 'sidetracks', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLBORE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrec as record_id,
        
        -- Wellbore identification
        des as wellbore_name,
        wellboreida as wellbore_api_uwi,
        wellboreidb as wellbore_id_b,
        wellboreidc as wellbore_id_c,
        wellboreidd as wellbore_id_d,
        wellboreide as wellbore_id_e,
        
        -- Parent relationship
        idrecparent as parent_wellbore_id,
        idrecparenttk as parent_wellbore_table_key,
        
        -- Wellbore characteristics
        purpose as purpose,
        profiletyp as profile_type,
        currentstatuscalc as current_status,
        dttmstatuscalc as current_status_date,
        
        -- Depths (converted to US units)
        depthstart / 0.3048 as start_depth_ft,
        depthtvdstartcalc / 0.3048 as start_depth_tvd_ft,
        depthtopactualcalc / 0.3048 as actual_top_depth_ft,
        depthtoppropcalc / 0.3048 as proposed_top_depth_ft,
        totaldepthcalc / 0.3048 as total_depth_ft,
        totaldepthtvdcalc / 0.3048 as total_depth_tvd_ft,
        depthdraw / 0.3048 as draw_depth_ft,
        depthkickoffmincalc / 0.3048 as min_kickoff_depth_ft,
        depthtvdkickoffmincalc / 0.3048 as min_kickoff_depth_tvd_ft,
        
        -- Drilling dates and duration
        dttmstartcalc as start_drill_date,
        dttmendcalc as end_drill_date,
        dttmkickoffmincalc as min_kickoff_date,
        durationcalc / 0.0416666666666667 as duration_hours,
        
        -- Directional information
        vsdir as vertical_section_direction_degrees,
        vsoriginns / 0.3048 as vertical_section_origin_ns_ft,
        vsoriginew / 0.3048 as vertical_section_origin_ew_ft,
        closedircalc as closure_direction_degrees,
        inclmaxcalc as max_inclination_degrees,
        dlsmaxcalc / 0.0328083989501312 as max_dls_degrees_per_100ft,
        
        -- Directional calculated values (converted to US units)
        tvdmincalc / 0.3048 as min_tvd_ft,
        tvdmaxcalc / 0.3048 as max_tvd_ft,
        vsplotmincalc / 0.3048 as min_vs_for_wellbore_ft,
        vsplotmaxcalc / 0.3048 as max_vs_for_wellbore_ft,
        departmaxcalc / 0.3048 as max_departure_ft,
        nsmincalc / 0.3048 as min_ns_for_wellbore_ft,
        nsmaxcalc / 0.3048 as max_ns_for_wellbore_ft,
        ewmincalc / 0.3048 as min_ew_for_wellbore_ft,
        ewmaxcalc / 0.3048 as max_ew_for_wellbore_ft,
        displaceunwrapcalc / 0.3048 as unwrapped_displacement_ft,
        
        -- Sensor durations (converted to minutes)
        duronbtmcalc / 0.000694444444444444 as duration_on_bottom_minutes,
        duroffbtmcalc / 0.000694444444444444 as duration_off_bottom_minutes,
        durpipemovingcalc / 0.000694444444444444 as duration_pipe_moving_minutes,
        
        -- Bottom hole location
        legalsurveytyp as legal_survey_type,
        legalsurveysubtyp as legal_survey_subtype,
        locationnote as location_note,
        legalsurveyloc as bottom_hole_legal_location,
        nsdist / 0.3048 as north_south_distance_ft,
        nsflag as north_south_flag,
        ewdist / 0.3048 as east_west_distance_ft,
        ewflag as east_west_flag,
        locationref as location_reference,
        county as county,
        stateprov as state_province,
        fieldname as field_name,
        fieldcode as field_code,
        
        -- Nearest town information
        townname as nearest_town,
        towndist / 1609.344 as distance_to_nearest_town_miles,
        townflag as nearest_town_ref_direction,
        townstateprov as nearest_town_state_prov,
        
        -- Geographic coordinates
        latlongsource as lat_long_data_source,
        latlongdatum as lat_long_datum,
        latitude as latitude_degrees,
        longitude as longitude_degrees,
        utmsource as utm_source,
        utmgridzone as utm_grid_zone,
        utmx as utm_easting_meters,
        utmy as utm_northing_meters,
        
        -- Job reference
        idrecjob as job_id,
        idrecjobtk as job_table_key,
        
        -- Configuration
        exclude as exclude_from_calculations,
        
        -- User fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        
        -- Comments
        com as comment,
        
        -- Sequence
        sysseq as sequence_number,

        -- System locking fields
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,

        -- System tracking fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,

        -- Fivetran metadata
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed