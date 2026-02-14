{{ config(
    materialized='view',
    tags=['wellview', 'tubing', 'components', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as component_id,
        idrecparent as tubing_string_id,
        idwell as well_id,
        sysseq as sequence_number,

        -- Basic component information
        des as description,
        iconname as icon_name,
        compsubtyp as equipment_type,
        itemnocalc as item_number,
        desjtcalc as description_with_joints,

        -- Dimensions (converted to US units)
        joints as joint_count,
        jointstallycalc as tally_joint_count,
        grade as steel_grade,
        material as material_type,
        make as manufacturer,
        model as model,
        sn as serial_number,
        usedclass as used_class,

        -- Depths (converted to US units)
        refid as reference_id,
        conntyptop as top_connection_type,
        connthrdtop as top_connection_thread,
        upsettop as top_upset,
        conntypbtm as bottom_connection_type,

        -- Joint information
        connthrdbtm as bottom_connection_thread,
        upsetbtm as bottom_upset,

        -- Material and specifications
        connectcalc as connection_description,
        connectaltcalc as alternative_connection_description,
        dttmmanufacture as manufacture_date,
        currentstatus as current_status,
        currentstatuscalc as current_status_calc,
        dttmstatuscalc as status_date,
        conditionrun as condition_run,

        -- Connection information - Top
        conditionpull as condition_pull,
        incltopcalc as top_inclination_degrees,
        inclbtmcalc as bottom_inclination_degrees,
        inclmaxcalc as max_inclination_degrees,

        -- Connection information - Bottom
        centralizersnotallycalc as centralizer_count,
        coatinginner as inner_coating,
        coatingouter as outer_coating,
        radioactivesource as is_radioactive_source,

        -- Connection calculations
        linetosurf as line_to_surface,
        cost as component_cost,

        -- Weights (converted to US units)
        costunitlabel as cost_unit_label,
        com as comments,

        -- Volumes (converted to US units)
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        syscreatedate as created_at,

        -- Torque specifications (converted to US units)
        syscreateuser as created_by,
        sysmoddate as modified_at,

        -- Pressure ratings (converted to US units)
        sysmoduser as modified_by,
        systag as system_tag,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,

        -- Tensile strength (converted to US units)
        syslockme as system_lock_me,

        -- Temperature rating (converted to Fahrenheit)
        syslockchildren as system_lock_children,

        -- Fishing neck specifications (converted to US units)
        syslockdate as system_lock_date,
        _fivetran_synced as fivetran_synced_at,

        -- Manufacturing and status
        szodnom / 0.0254 as od_nominal_inches,
        szidnom / 0.0254 as id_nominal_inches,
        szodmax / 0.0254 as od_max_inches,
        szdrift / 0.0254 as drift_diameter_inches,
        wtperlength / 1.48816394356955 as weight_per_foot_lbs,
        length / 0.3048 as length_ft,

        -- Operational hours (converted to hours)
        lengthcumcalc / 0.3048 as cumulative_length_ft,

        -- Inclination data (degrees)
        lengthtallycalc / 0.3048 as tally_length_ft,
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,

        -- Centralizers
        depthtopcorrected / 0.3048 as corrected_top_depth_ft,

        -- Special features and coatings
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        connsztop / 0.0254 as top_connection_size_inches,
        connszbtm / 0.0254 as bottom_connection_size_inches,

        -- Cost information
        weightcalc / 4.4482216152605 as component_weight_lbf,
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf,

        -- Comments
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,

        -- Last failure reference
        volumedispcalc / 0.158987294928 as displaced_volume_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_displaced_volume_bbl,

        -- System fields
        torquemin / 1.3558179483314 as makeup_torque_min_ft_lbs,
        torquemax / 1.3558179483314 as max_torque_ft_lbs,
        prescollapse / 6.894757 as collapse_pressure_psi,
        presburst / 6.894757 as burst_pressure_psi,
        presaxialinner / 6.894757 as axial_inner_pressure_psi,
        presaxialouter / 6.894757 as axial_outer_pressure_psi,
        tensilemax / 4448.2216152605 as max_tensile_strength_klbf,
        temprating / 0.555555555555556 + 32 as temperature_rating_fahrenheit,
        fishneckod / 0.0254 as fishing_neck_od_inches,
        fishnecklength / 0.3048 as fishing_neck_length_ft,

        -- Fivetran metadata
        hoursstart / 0.0416666666666667 as starting_hours

    from source_data
)

select * from renamed
