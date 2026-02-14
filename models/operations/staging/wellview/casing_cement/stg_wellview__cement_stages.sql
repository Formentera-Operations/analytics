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
        measmethod as top_measurement_method,
        dttmstartpump as pump_start_date,
        dttmendpump as pump_end_date,
        topplug as top_plug,
        btmplug as bottom_plug,

        -- Pumping timing
        dttmreleasedpres as pressure_release_date,
        plugfailed as plug_failed,

        -- Plug configuration
        floatfailed as float_failed,
        fullreturn as full_return,

        -- Pump rates (converted to BBL/MIN)
        reciprocated as pipe_reciprocated,
        reciprate as reciprocation_rate_spm,
        rotated as pipe_rotated,

        -- Pressures (converted to PSI)
        rotaterpm as pipe_rpm,
        pipemovenote as pipe_movement_note,
        tagmethod as tag_method,
        dttmtagged as tag_cement_date,

        -- Operational flags
        dttmdrillout as drill_out_date,
        dttmpropdrillout as proposed_drill_out_date,
        com as comment,

        -- Volumes (converted to barrels)
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        syslockme as system_lock_me,

        -- Pipe movement
        syslockchildren as system_lock_children,
        syslockdate as system_lock_date,
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,

        -- Cement plug operations
        systag as system_tag,
        _fivetran_synced as fivetran_synced_at,
        depthtop / 0.3048 as top_depth_ft,
        depthbtm / 0.3048 as bottom_depth_ft,
        depthtvdtopcalc / 0.3048 as top_depth_tvd_ft,
        depthtvdbtmcalc / 0.3048 as bottom_depth_tvd_ft,
        ratepumpstart / 228.941712 as initial_pump_rate_bbl_per_min,
        ratepumpend / 228.941712 as final_pump_rate_bbl_per_min,

        -- Proposed operations
        ratepumpavg / 228.941712 as avg_pump_rate_bbl_per_min,

        -- Comments
        prespumpend / 6.894757 as final_pump_pressure_psi,

        -- System locking fields
        presplugbump / 6.894757 as plug_bump_pressure_psi,
        presheld / 6.894757 as pressure_held_psi,
        volreturncmnt / 0.158987294928 as cement_volume_return_bbl,
        vollost / 0.158987294928 as volume_lost_bbl,
        volinfrm / 0.158987294928 as volume_squeezed_in_to_formation_bbl,

        -- System tracking fields
        recipstroke / 0.3048 as reciprocation_stroke_length_ft,
        depthtagged / 0.3048 as tagged_depth_ft,
        weighttagged / 4448.2216152605 as tag_weight_1000_lbf,
        depthdrillout / 0.3048 as depth_plug_drilled_out_to_ft,
        oddrillout / 0.0254 as drill_out_diameter_inches,

        -- Fivetran metadata
        durdrillouttopumpendcalc / 0.0416666666666667 as drill_out_to_pump_end_duration_hours

    from source_data
)

select * from renamed
