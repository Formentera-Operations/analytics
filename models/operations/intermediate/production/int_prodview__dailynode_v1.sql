{{
  config(
    materialized='view',
    alias='dailynode_v1'
  )
}}

with pvunitnodemonthdaycalc as (
    select * from {{ ref('stg_prodview__pvunitnodemonthdaycalc') }}
    where deleted = false
)

select
    -- Key IDs
    id_rec,
    id_rec_node as node_id,
    id_rec_facility as facility_id,
    
    -- Time period
    dttm,
    year,
    month,
    day_of_month,
    
    -- Volume data (already converted to standard units)
    vol_gas,
    vol_hc_liq,
    vol_hc_liq_gas_eq,
    vol_sand,
    vol_water,
    
    -- Heat content data
    heat,
    fact_heat,
    
    -- System audit fields
    sys_create_date,
    sys_create_user,
    sys_mod_date,
    sys_mod_user,
    
    -- Update tracking
    update_date

from pvunitnodemonthdaycalc