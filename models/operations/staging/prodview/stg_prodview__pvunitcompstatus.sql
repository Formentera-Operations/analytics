{{
  config(
    materialized='view',
    alias='pvunitcompstatus'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPSTATUS') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        DTTM as dttm,
        
        -- Status and operational configuration
        STATUS as status,
        PRIMARYFLUIDTYP as primary_fluid_type,
        FLOWDIRECTION as flow_direction,
        COMMINGLED as commingled,
        TYPFLUIDPROD as type_fluid_production,
        TYPCOMPLETION as type_completion,
        METHODPROD as method_production,
        
        -- Calculation settings
        CALCLOSTPROD as calc_lost_production,
        WELLCOUNTINCL as well_count_included,
        
        -- User-defined text fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        
        -- User-defined numeric fields
        USERNUM1 as user_num_1,
        USERNUM2 as user_num_2,
        USERNUM3 as user_num_3,
        
        -- General information
        COM as com,
        
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