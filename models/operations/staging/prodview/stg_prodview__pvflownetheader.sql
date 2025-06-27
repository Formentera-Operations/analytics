{{
  config(
    materialized='view',
    alias='pvflownetheader'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVFLOWNETHEADER') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        NAME as name,
        TYP as type,
        
        -- Primary unit and facility references
        IDRECUNITPRIMARY as id_rec_unit_primary,
        IDRECUNITPRIMARYTK as id_rec_unit_primary_tk,
        IDRECFACILITYPRIMARY as id_rec_facility_primary,
        IDRECFACILITYPRIMARYTK as id_rec_facility_primary_tk,
        
        -- General information
        COM as com,
        
        -- Responsibility assignments
        IDRECRESP1 as id_rec_resp_1,
        IDRECRESP1TK as id_rec_resp_1_tk,
        IDRECRESP2 as id_rec_resp_2,
        IDRECRESP2TK as id_rec_resp_2_tk,
        
        -- Reporting configuration flags
        RPTGATHEREDCALCS as rpt_gathered_calcs,
        RPTALLOCATIONS as rpt_allocations,
        RPTDISPOSITIONS as rpt_dispositions,
        RPTCOMPONENTDISPOSITIONS as rpt_component_dispositions,
        RPTNODECALCULATIONS as rpt_node_calculations,
        
        -- Operational settings
        TRACKDOWNHOLEINVENTORY as track_downhole_inventory,
        
        -- Allocation and process dates
        DTTMALLOCPROCESSBEGAN as dttm_alloc_process_began,
        DTTMSTART as dttm_start,
        DTTMEND as dttm_end,
        DTTMLASTALLOCPROCESS as dttm_last_alloc_process,
        USERLASTALLOCPROCESS as user_last_alloc_process,
        
        -- User-defined text fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        USERTXT4 as user_txt_4,
        USERTXT5 as user_txt_5,
        
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
        
        -- Additional system fields
        SYSMODDATEDB as sys_mod_date_db,
        SYSMODUSERDB as sys_mod_user_db,
        SYSSECURITYTYP as sys_security_type,
        SYSLOCKDATEMASTER as sys_lock_date_master,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed