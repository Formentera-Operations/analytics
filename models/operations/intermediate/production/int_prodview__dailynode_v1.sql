{{
  config(
    materialized='view',
    alias='dailynode_v1'
  )
}}

with pvunitnodemonthdaycalc as (
    select * from {{ ref('stg_prodview__node_daily_volumes') }}
    where DELETED = false
)

select
    -- Key IDs
    IDREC,
    IDRECNODE,
    IDRECFACILITY,
    
    -- Time period
    DTTM,
    YEAR,
    MONTH,
    DAYOFMONTH,
    
    -- Volume data (already converted to standard units)
    VOLGAS,
    VOLHCLIQ,
    VOLHCLIQGASEQ,
    VOLSAND,
    VOLWATER,
    
    -- Heat content data
    HEAT,
    FACTHEAT,
    
    -- System audit fields
    SYSCREATEDATE,
    SYSCREATEUSER,
    SYSMODDATE,
    SYSMODUSER,
    
    -- Update tracking
    UPDATE_DATE

from pvunitnodemonthdaycalc