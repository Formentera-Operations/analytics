{{
  config(
    materialized='view',
    alias='pvunitcompdowntm'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPDOWNTM') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        
        -- Downtime configuration
        TYPDOWNTM as type_downtime,
        DTTMSTART as dttm_start,
        DTTMEND as dttm_end,
        DTTMPLANEND as dttm_plan_end,
        
        -- Duration fields with unit conversion (minutes to hours)
        DURDOWNSTARTDAY / 0.0416666666666667 as dur_down_start_day,
        case when DURDOWNSTARTDAY is not null then 'HR' else null end as dur_down_start_day_unit_label,
        DURDOWNENDDAY / 0.0416666666666667 as dur_down_end_day,
        case when DURDOWNENDDAY is not null then 'HR' else null end as dur_down_end_day_unit_label,
        DURDOWNCALC / 0.0416666666666667 as dur_down_calc,
        case when DURDOWNCALC is not null then 'HR' else null end as dur_down_calc_unit_label,
        DURDOWNPLANEND / 0.0416666666666667 as dur_down_plan_end,
        case when DURDOWNPLANEND is not null then 'HR' else null end as dur_down_plan_end_unit_label,
        
        -- Downtime codes
        CODEDOWNTM1 as code_downtime_1,
        CODEDOWNTM2 as code_downtime_2,
        CODEDOWNTM3 as code_downtime_3,
        
        -- Additional information
        COM as com,
        LOCATION as location,
        FAILFLAG as fail_flag,
        PRODUCT as product,
        
        -- User-defined text fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        USERTXT4 as user_txt_4,
        USERTXT5 as user_txt_5,
        
        -- User-defined numeric fields
        USERNUM1 as user_num_1,
        USERNUM2 as user_num_2,
        USERNUM3 as user_num_3,
        USERNUM4 as user_num_4,
        USERNUM5 as user_num_5,
        
        -- User-defined datetime fields
        USERDTTM1 as user_dttm_1,
        USERDTTM2 as user_dttm_2,
        USERDTTM3 as user_dttm_3,
        USERDTTM4 as user_dttm_4,
        USERDTTM5 as user_dttm_5,
        
        -- System locking fields
        SYSLOCKMEUI as sys_lock_me_ui,
        SYSLOCKCHILDRENUI as sys_lock_children_ui,
        SYSLOCKME as sys_lock_me,
        SYSLOCKCHILDREN as sys_lock_children,
        SYSLOCKDATE as sys_lock_date,
        
        -- System audit fields
        SYSMODDATE as sys_mod_date,
        SYSMODUSER as sys_mod_user,
        SYSCREATEDATE as sys_create_date,
        SYSCREATEUSER as sys_create_user,
        SYSTAG as sys_tag,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed