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
        szodnom / 0.0254 as od_nominal_inches,
        szidnom / 0.0254 as id_nominal_inches,
        szodmax / 0.0254 as od_max_inches,
        szdrift / 0.0254 as drift_diameter_inches,
        wtperlength / 1.48816394356955 as weight_per_foot_lbs,
        length / 0.3048 as length_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        lengthtallycalc / 0.3048 as tally_length_ft,
        
        -- Depths (converted to US units)
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtopcorrected / 0.3048 as corrected_top_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        
        -- Joint information
        joints as joint_count,
        jointstallycalc as tally_joint_count,
        
        -- Material and specifications
        grade as steel_grade,
        material as material_type,
        make as manufacturer,
        model as model,
        sn as serial_number,
        usedclass as used_class,
        refid as reference_id,
        
        -- Connection information - Top
        conntyptop as top_connection_type,
        connthrdtop as top_connection_thread,
        connsztop / 0.0254 as top_connection_size_inches,
        upsettop as top_upset,
        
        -- Connection information - Bottom
        conntypbtm as bottom_connection_type,
        connthrdbtm as bottom_connection_thread,
        connszbtm / 0.0254 as bottom_connection_size_inches,
        upsetbtm as bottom_upset,
        
        -- Connection calculations
        connectcalc as connection_description,
        connectaltcalc as alternative_connection_description,
        
        -- Weights (converted to US units)
        weightcalc / 4.4482216152605 as component_weight_lbf,
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf,
        
        -- Volumes (converted to US units)
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumedispcalc / 0.158987294928 as displaced_volume_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_displaced_volume_bbl,
        
        -- Torque specifications (converted to US units)
        torquemin / 1.3558179483314 as makeup_torque_min_ft_lbs,
        torquemax / 1.3558179483314 as max_torque_ft_lbs,
        
        -- Pressure ratings (converted to US units)
        prescollapse / 6.894757 as collapse_pressure_psi,
        presburst / 6.894757 as burst_pressure_psi,
        presaxialinner / 6.894757 as axial_inner_pressure_psi,
        presaxialouter / 6.894757 as axial_outer_pressure_psi,
        
        -- Tensile strength (converted to US units)
        tensilemax / 4448.2216152605 as max_tensile_strength_klbf,
        
        -- Temperature rating (converted to Fahrenheit)
        temprating / 0.555555555555556 + 32 as temperature_rating_fahrenheit,
        
        -- Fishing neck specifications (converted to US units)
        fishneckod / 0.0254 as fishing_neck_od_inches,
        fishnecklength / 0.3048 as fishing_neck_length_ft,
        
        -- Manufacturing and status
        dttmmanufacture as manufacture_date,
        currentstatus as current_status,
        currentstatuscalc as current_status_calc,
        dttmstatuscalc as status_date,
        conditionrun as condition_run,
        conditionpull as condition_pull,
        
        -- Operational hours (converted to hours)
        hoursstart / 0.0416666666666667 as starting_hours,
        
        -- Inclination data (degrees)
        incltopcalc as top_inclination_degrees,
        inclbtmcalc as bottom_inclination_degrees,
        inclmaxcalc as max_inclination_degrees,
        
        -- Centralizers
        centralizersnotallycalc as centralizer_count,
        
        -- Special features and coatings
        coatinginner as inner_coating,
        coatingouter as outer_coating,
        radioactivesource as is_radioactive_source,
        linetosurf as line_to_surface,
        
        -- Cost information
        cost as component_cost,
        costunitlabel as cost_unit_label,
        
        -- Comments
        com as comments,
        
        -- Last failure reference
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
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