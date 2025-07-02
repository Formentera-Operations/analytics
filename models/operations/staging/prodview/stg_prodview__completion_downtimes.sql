{{ config(
    materialized='view',
    tags=['prodview', 'downtime', 'completions', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPDOWNTM') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as downtime_id,
        idrecparent as completion_id,
        idflownet as flow_network_id,
        
        -- Downtime classification
        typdowntm as type_of_downtime_entry,
        product as product,
        location as location,
        failflag as is_failure,
        
        -- Downtime period - Start
        dttmstart as first_day,
        durdownstartday / 0.0416666666666667 as hours_down_start_day,
        
        -- Downtime period - End
        dttmend as last_day,
        durdownendday / 0.0416666666666667 as downtime_on_last_day_hours,
        
        -- Total downtime calculation
        durdowncalc / 0.0416666666666667 as total_downtime_hours,
        
        -- Planned downtime
        dttmplanend as planned_end_date,
        durdownplanend / 0.0416666666666667 as planned_downtime_duration_hours,
        
        -- Downtime codes
        codedowntm1 as downtime_code,
        codedowntm2 as downtime_code_2,
        codedowntm3 as downtime_code_3,
        
        -- Comments and notes
        com as comments,
        
        -- User-defined fields - Text
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        
        -- User-defined fields - Numeric
        usernum1 as user_num_1,
        usernum2 as user_num_2,
        usernum3 as user_num_3,
        usernum4 as user_num_4,
        usernum5 as user_num_5,
        
        -- User-defined fields - Datetime
        userdttm1 as user_date_1,
        userdttm2 as user_date_2,
        userdttm3 as user_date_3,
        userdttm4 as user_date_4,
        userdttm5 as user_date_5,
        
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