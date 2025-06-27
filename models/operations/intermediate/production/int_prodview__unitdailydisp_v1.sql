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
    IDRECDISPUNITNODE AS DISPNODEID,
    IDRECDISPUNIT AS DISPUNITID,
    IDREC AS IDREC,
    IDRECUNIT AS UNITID,
    
    -- Time period
    DTTM,
    
    -- Heat content
    HEAT,
    
    -- System audit fields
    SYSCREATEDATE,
    SYSCREATEUSER,
    SYSMODDATE,
    SYSMODUSER,
    
    -- Hydrocarbon component volumes (liquid phase)
    VOLC1LIQ,
    VOLC2LIQ,
    VOLC3LIQ,
    VOLIC4LIQ,
    VOLNC4LIQ,
    VOLIC5LIQ,
    VOLNC5LIQ,
    VOLC6LIQ,
    VOLC7LIQ,
    
    -- Total fluid volumes
    VOLGAS,
    VOLHCLIQ,
    VOLWATER,
    
    -- Update tracking
    UPDATE_DATE

from pvunitdispmonthday