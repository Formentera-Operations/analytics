{{ config(
    materialized='view',
    tags=['wellview', 'cement', 'stages', 'pumping', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVCEMENTSTAGE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idwell as well_id,
        idrecparent as parent_record_id,
        idrec as record_id,
        
        -- Stage information
        stagenum as stage_number,
        des as description,
        objective as objective,
        
        -- Depths (converted to US units)
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        measmethod as top_measurement_method,
        
        -- Pumping timing
        dttmstartpump as pump_start_date,
        dttmendpump as pump_end_date,
        
        -- Plug configuration
        topplug as top_plug,
        btmplug as bottom_plug,
        
        -- Pump rates (converted to BBL/MIN)
        ratepumpstart / 228.941712 as initial_pump_rate_bbl_per_min,
        ratepumpend / 228.941712 as final_pump_rate_bbl_per_min,
        ratepumpavg / 228.941712 as avg_pump_rate_bbl_per_min,
        
        -- Pressures (converted to PSI)
        prespumpend / 6.894757 as final_pump_pressure_psi,
        presplugbump / 6.894757 as plug_bump_pressure_psi,
        presheld / 6.894757 as pressure_held_psi,
        dttmreleasedpres as pressure_release_date,
        
        -- Operational flags
        plugfailed as plug_failed,
        floatfailed as float_failed,
        fullreturn as full_return,
        
        -- Volumes (converted to barrels)
        volreturncmnt / 0.158987294928 as cement_volume_return_bbl,
        vollost / 0.158987294928 as volume_lost_bbl,
        volinfrm / 0.158987294928 as volume_squeezed_in_to_formation_bbl,
        
        -- Pipe movement
        reciprocated as pipe_reciprocated,
        recipstroke / 0.3048 as reciprocation_stroke_length_ft,
        reciprate as reciprocation_rate_spm,
        rotated as pipe_rotated,
        rotaterpm as pipe_rpm,
        pipemovenote as pipe_movement_note,
        
        -- Cement plug operations
        depthtagged / 0.3048 as tagged_depth_ft,
        tagmethod as tag_method,
        weighttagged / 4448.2216152605 as tag_weight_1000_lbf,
        depthdrillout / 0.3048 as depth_plug_drilled_out_to_ft,
        dttmtagged as tag_cement_date,
        dttmdrillout as drill_out_date,
        oddrillout / 0.0254 as drill_out_diameter_inches,
        durdrillouttopumpendcalc / 0.0416666666666667 as drill_out_to_pump_end_duration_hours,
        
        -- Proposed operations
        dttmpropdrillout as proposed_drill_out_date,
        
        -- Comments
        com as comment,

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