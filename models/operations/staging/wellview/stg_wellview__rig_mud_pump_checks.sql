{{ config(
    materialized='view',
    tags=['wellview', 'rig', 'pump', 'checks', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBRIGPUMPCHK') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as pump_check_id,
        idwell as well_id,
        idrecparent as rig_pump_id,
        
        -- Check timing and location
        dttm as check_datetime,
        depth / 0.3048 as depth_ft,
        
        -- Pump operation parameters
        case when slowspeed = 1 then true else false end as is_slow_speed_check,
        spm as strokes_per_minute,
        pumpingmode as pumping_mode,
        
        -- Performance metrics (converted to US units)
        volefficiency / 0.01 as volumetric_efficiency_percent,
        flowratecalc / 5.45099328 as calculated_flow_rate_gpm,
        
        -- Pressure readings (converted to US units)
        pres / 6.894757 as pressure_psi,
        presclf / 6.894757 as choke_line_friction_pressure_psi,
        presklf / 6.894757 as kill_line_friction_pressure_psi,
        
        -- Related data references
        idreclastmudchkcalc as last_mud_check_id,
        idreclastmudchkcalctk as last_mud_check_table_key,
        
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