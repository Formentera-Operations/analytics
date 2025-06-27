{{
  config(
    materialized='view',
    alias='unitdailydisp_v1'
  )
}}

with pvunitdispmonthday as (
    select * from {{ ref('stg_prodview__pvunitdispmonthday') }}
    where deleted = false
)

select
    -- Key IDs
    id_rec_disp_unit_node as disp_node_id,
    id_rec_disp_unit as disp_unit_id,
    id_rec,
    id_rec_unit as unit_id,
    
    -- Time period
    dttm,
    
    -- Heat content
    heat,
    
    -- System audit fields
    sys_create_date,
    sys_create_user,
    sys_mod_date,
    sys_mod_user,
    
    -- Hydrocarbon component volumes (liquid phase)
    vol_c1_liq,
    vol_c2_liq,
    vol_c3_liq,
    vol_ic4_liq,
    vol_nc4_liq,
    vol_ic5_liq,
    vol_nc5_liq,
    vol_c6_liq,
    vol_c7_liq,
    
    -- Total fluid volumes
    vol_gas,
    vol_hc_liq,
    vol_water,
    
    -- Update tracking
    update_date

from pvunitdispmonthday