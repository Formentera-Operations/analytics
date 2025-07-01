{{ config(
    materialized='view',
    tags=['wellview', 'casing', 'components', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCASCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as component_id,
        idrecparent as casing_string_id,
        idwell as well_id,
        sysseq as component_sequence,
        
        -- Basic component information
        des as component_description,
        compsubtyp as component_subtype,
        iconname as icon_name,
        com as comments,
        
        -- Physical specifications (converted to US units)
        szodnom / 0.0254 as nominal_od_in,
        szidnom / 0.0254 as nominal_id_in,
        szodmax / 0.0254 as max_od_in,
        szdrift / 0.0254 as drift_diameter_in,
        length / 0.3048 as component_length_ft,
        wtperlength / 1.48816394356955 as weight_per_length_lb_per_ft,
        
        -- Depths and positions (converted to US units)
        depthtopcalc / 0.3048 as top_depth_ft,
        depthbtmcalc / 0.3048 as bottom_depth_ft,
        depthtopcorrected / 0.3048 as top_depth_corrected_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        lengthcumcalc / 0.3048 as cumulative_length_ft,
        lengthtallycalc / 0.3048 as tally_length_ft,
        
        -- Inclinations (already in degrees)
        incltopcalc as top_inclination_deg,
        inclbtmcalc as bottom_inclination_deg,
        inclmaxcalc as max_inclination_deg,
        
        -- Joints and quantities
        joints as joint_count,
        jointstallycalc as joints_in_tally,
        centralizersnotallycalc as centralizer_count_tally,
        
        -- Weights and forces (converted to US units)
        weightcalc / 4448.2216152605 as component_weight_kips,
        weightcumcalc / 4448.2216152605 as cumulative_weight_klbf,
        tensilemax / 4448.2216152605 as max_tensile_strength_klbf,
        
        -- Torque specifications (converted to US units)
        torquemin / 1.3558179483314 as min_makeup_torque_ft_lb,
        torquemax / 1.3558179483314 as max_torque_ft_lb,
        
        -- Pressure ratings (converted to US units)
        presburst / 6.894757 as burst_pressure_psi,
        prescollapse / 6.894757 as collapse_pressure_psi,
        presaxialinner / 6.894757 as axial_inner_pressure_psi,
        presaxialouter / 6.894757 as axial_outer_pressure_psi,
        
        -- Volumes (converted to US units)
        volumeinternalcalc / 0.158987294928 as internal_volume_bbl,
        volumedispcalc / 0.158987294928 as displaced_volume_bbl,
        volumedispcumcalc / 0.158987294928 as cumulative_displaced_volume_bbl,
        
        -- Connection specifications
        conntyptop as top_connection_type,
        conntypbtm as bottom_connection_type,
        connthrdtop as top_connection_thread,
        connthrdbtm as bottom_connection_thread,
        connsztop / 0.0254 as top_connection_size_in,
        connszbtm / 0.0254 as bottom_connection_size_in,
        conntgtperftop as top_connection_target_performance,
        conntgtperfbtm as bottom_connection_target_performance,
        upsettop as top_upset,
        upsetbtm as bottom_upset,
        connectcalc as connection_info,
        connectaltcalc as connection_info_alt,
        
        -- Material and manufacturing
        grade as component_grade,
        material as material_specification,
        make as manufacturer,
        model as component_model,
        heatrating as heat_rating,
        usedclass as used_class,
        sn as serial_number,
        refid as reference_id,
        
        -- Dates
        dttmmanufacture as manufacture_datetime,
        dttmstatuscalc as status_datetime,
        
        -- Cost information
        cost as component_cost,
        costunitlabel as cost_unit_label,
        
        -- Calculated fields
        itemnocalc as item_number,
        desjtcalc as description_with_joints,
        currentstatuscalc as current_status,
        
        -- Reference IDs
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at
        
    from source_data
)

select * from renamed