{{
  config(
    materialized='view',
    alias='flownetwork_v1'
  )
}}

with pvflownetheader as (
    select * from {{ ref('stg_prodview__pvflownetheader') }}
    where deleted = false
)

select
    -- General information
    com,
    dttm_end,
    dttm_start,
    dttm_last_alloc_process,
    id_flow_net as flow_net_id,
    name,
    id_rec_resp_1 as primary_resp_team_id,
    
    -- Reporting configuration flags
    rpt_allocations,
    rpt_component_dispositions,
    rpt_dispositions,
    rpt_gathered_calcs,
    rpt_node_calculations,
    
    -- System audit fields
    sys_create_date,
    sys_create_user,
    sys_mod_date,
    sys_mod_user,
    
    -- Flow network type and configuration
    type,
    
    -- User-defined text fields
    user_txt_1,
    user_txt_2,
    user_txt_3,
    user_txt_4,
    user_txt_5,
    
    -- Update tracking
    update_date

from pvflownetheader