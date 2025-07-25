{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'plunger_lift', 'entries', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPPUMPPLUNGERENTRY') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as plunger_entry_id,
        idrecparent as plunger_lift_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttm as entry_date,
        
        -- Operating parameters
        plungeronpressure / 6.894757 as plunger_on_pressure_psi,
        
        -- Trip counts
        tripcounttotal as total_trips,
        tripcountsuccess as successful_trips,
        tripcountfail as failed_trips,
        
        -- Duration metrics (converted from days to appropriate units)
        duron / 0.000694444444444444 as duration_on_minutes,
        duroff / 0.000694444444444444 as duration_off_minutes,
        afterflowtime / 0.0416666666666667 as after_flow_time_hours,
        
        -- Travel time metrics (converted from days to minutes)
        traveltimeavg / 0.000694444444444444 as travel_time_avg_minutes,
        traveltimemax / 0.000694444444444444 as travel_time_max_minutes,
        traveltimemin / 0.000694444444444444 as travel_time_min_minutes,
        traveltimetarget / 0.000694444444444444 as travel_time_target_minutes,
        
        -- Notes and comments
        com as comments,
        
        -- User fields
        usertxt1 as plunger_make,
        usertxt2 as plunger_model,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        userdttm1 as plunger_inspection_date,
        userdttm2 as plunger_replace_date,
        userdttm3 as user_date_3,
        
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