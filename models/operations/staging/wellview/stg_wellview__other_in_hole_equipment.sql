{{ config(
    materialized='view',
    tags=['wellview', 'other_in_hole', 'equipment', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVOTHERINHOLE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as other_in_hole_id,
        idwell as well_id,
        
        -- Basic equipment information
        proposedoractual as proposed_or_actual,
        propversionno as proposed_version_number,
        des as equipment_description,
        compsubtyp as equipment_type,
        iconname as icon_name,
        
        -- Manufacturer information
        make as manufacturer,
        model as equipment_model,
        sn as serial_number,
        material as equipment_material,
        coating as equipment_coating,
        refid as reference_id,
        
        -- Physical dimensions (converted to US units)
        szodnom / 0.0254 as nominal_outer_diameter_in,
        szidnom / 0.0254 as nominal_inner_diameter_in,
        szodmax / 0.0254 as maximum_outer_diameter_in,
        szdrift / 0.0254 as drift_diameter_in,
        lengthcalc / 0.3048 as equipment_length_ft,
        
        -- Fishing specifications (converted to US units)
        fishneckod / 0.0254 as fishing_neck_od_in,
        fishnecklength / 0.3048 as fishing_neck_length_ft,
        
        -- Depths and positions (converted to US units)
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        depthtopcorrected / 0.3048 as corrected_top_depth_ft,
        latposition / 0.0254 as lateral_position_in,
        
        -- Inclination data (no conversion needed - already in degrees)
        incltopcalc as top_inclination_degrees,
        inclbtmcalc as bottom_inclination_degrees,
        inclmaxcalc as maximum_inclination_degrees,
        
        -- Tension and pressure ratings (converted to US units)
        tensionpreset / 4.4482216152605 as tension_pre_set_lbf,
        tensionpostset / 4.4482216152605 as tension_post_set_lbf,
        presrating / 6.894757 as pressure_rating_psi,
        temprating / 0.555555555555556 + 32 as temperature_rating_f,
        
        -- Operational dates
        dttmrun as run_datetime,
        dttmpickup as pickup_datetime,
        dttmonbottom as on_bottom_datetime,
        dttmoutofhole as out_of_hole_datetime,
        dttmmanufacture as manufacture_datetime,
        
        -- Operational conditions
        conditionrun as condition_run,
        conditionpull as condition_pull,
        hoursstart / 0.0416666666666667 as starting_hours,
        
        -- Status and current condition
        currentstatuscalc as current_status,
        dttmstatuscalc as current_status_datetime,
        case when radioactivesource = 1 then true else false end as is_radioactive_source,
        
        -- Pull information
        dttmpull as pull_datetime,
        dttmproppull as proposed_pull_datetime,
        pullreason as pull_reason,
        pullreasondetail as pull_reason_detail,
        
        -- Calculated durations (converted to US units)
        duronbottomtopickupcalc / 0.0416666666666667 as duration_on_bottom_to_pickup_hours,
        durruntopullcalc as duration_run_to_pull_days,
        depthonbtmtopickupcalc / 7.3152 as depth_on_bottom_to_pickup_ft_per_hour,
        
        -- Cost information
        cost as equipment_cost,
        costunitlabel as cost_unit_label,
        
        -- Related entities
        idrecwellbore as wellbore_id,
        idrecwellboretk as wellbore_table_key,
        idrecstring as string_id,
        idrecstringtk as string_table_key,
        idrecjobrun as run_job_id,
        idrecjobruntk as run_job_table_key,
        idrecjobpull as pull_job_id,
        idrecjobpulltk as pull_job_table_key,
        idrecjobprogramphasecalc as program_phase_id,
        idrecjobprogramphasecalctk as program_phase_table_key,
        idreclastrigcalc as last_rig_id,
        idreclastrigcalctk as last_rig_table_key,
        idreclastfailurecalc as last_failure_id,
        idreclastfailurecalctk as last_failure_table_key,
        
        -- Additional information
        complexityindex as complexity_index,
        com as comments,
        
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