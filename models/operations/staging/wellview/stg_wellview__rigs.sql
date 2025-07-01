{{ config(
    materialized='view',
    tags=['wellview', 'rig', 'job', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIG') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as job_rig_id,
        idwell as well_id,
        idrecparent as job_id,
        
        -- Basic rig information
        proposedoractual as proposed_or_actual,
        contractor as rig_contractor,
        contractorparent as contractor_parent,
        rigno as rig_number,
        typ1 as rig_type,
        typ2 as rig_subtype,
        contracttyp as contract_type,
        category as rig_category,
        purpose as rig_purpose,
        registration as registration_country,
        inventoryno as inventory_number,
        
        -- Contractor contact information
        idrecjobcontactcontractor as rig_supervisor_id,
        idrecjobcontactcontractortk as rig_supervisor_table_key,
        
        -- Date management
        dateortyp as date_override_type,
        dttmstart as rig_start_datetime,
        dttmend as rig_end_datetime,
        dttmstartorcalc as earliest_start_datetime,
        dttmendorcalc as latest_end_datetime,
        durcalc / 0.0416666666666667 as rig_duration_hours,
        
        -- Physical specifications (converted to US units)
        depthmax / 0.3048 as max_rated_depth_ft,
        refheight / 0.3048 as reference_height_ft,
        heightmastclearance / 0.3048 as mast_clearance_height_ft,
        heightsubclear / 0.3048 as sub_clearance_height_ft,
        
        -- Load and capacity ratings (converted to US units)
        hookloadmax / 4448.2216152605 as max_hook_load_klbf,
        weightblock / 4448.2216152605 as block_weight_klbf,
        setbackcapacity / 0.45359237 as setback_capacity_lb,
        transportloads as transport_loads_count,
        maxvariableload / 4448.2216152605 as max_variable_load_klbf,
        
        -- Torque and power (converted to US units)
        torquemax / 1.3558179483314 as max_torque_ft_lbf,
        power / 745.6999 as power_rating_hp,
        powertyp as power_type,
        
        -- Rotary system
        rotarysystem as rotary_system_type,
        
        -- Derrick and drawworks
        derricktyp as derrick_type,
        drawworktyp as drawworks_type,
        drawworkmake as drawworks_manufacturer,
        drawworkmodel as drawworks_model,
        
        -- Surface lines and volumes (converted to US units)
        lengthkillline / 0.3048 as kill_line_length_ft,
        szidkillline / 0.0254 as kill_line_id_in,
        volkilllinecalc / 0.158987294928 as kill_line_volume_bbl,
        lengthchokeline / 0.3048 as choke_line_length_ft,
        szidchokeline / 0.0254 as choke_line_id_in,
        volchokelinecalc / 0.158987294928 as choke_line_volume_bbl,
        volsurfline / 0.158987294928 as surface_line_volume_bbl,
        
        -- Offshore specifications (converted to US units)
        waterdepthmax / 0.3048 as max_water_depth_ft,
        postyp as positioning_system_type,
        slipjtextmax / 0.3048 as max_slip_joint_extension_ft,
        anchorno as anchor_count,
        anchortyp as anchor_type,
        anchorlinetyp as anchor_line_type,
        anchormaxtension / 4448.2216152605 as max_anchor_tension_klbf,
        
        -- Cost information
        rigrateref as rig_rate_reference,
        
        -- Calculated durations (converted to US units)
        duronbtmcalc / 0.000694444444444444 as duration_on_bottom_min,
        duroffbtmcalc / 0.000694444444444444 as duration_off_bottom_min,
        durpipemovingcalc / 0.000694444444444444 as duration_pipe_moving_min,
        
        -- Comments
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