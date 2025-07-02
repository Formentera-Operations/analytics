{{
  config(
    materialized='view',
    alias='flownetwork_v1'
  )
}}

with pvflownetheader as (
    select * from {{ ref('stg_prodview__networks') }}
    where DELETED = false
)

select
    -- General information
    COM,
    DTTMEND,
    DTTMSTART,
    DTTMALLOCPROCESSBEGAN,
    IDFLOWNET,
    NAME,
    IDRECRESP1,
    
    -- Reporting configuration flags
    RPTGATHEREDCALCS,
    RPTALLOCATIONS,
    RPTDISPOSITIONS,
    RPTCOMPONENTDISPOSITIONS,
    RPTNODECALCULATIONS,
    
    -- System audit fields
    SYSCREATEDATE,
    SYSCREATEUSER,
    SYSMODDATE,
    SYSMODUSER,
    
    -- Flow network type and configuration
    TYP,
    
    -- User-defined text fields
    USERTXT1,
    USERTXT2,
    USERTXT3,
    USERTXT4,
    USERTXT5,
    
    -- Update tracking
    UPDATE_DATE

from pvflownetheader