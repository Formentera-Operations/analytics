{{
  config(
    materialized='view',
    alias='unitnode_v1'
  )
}}

with pvunitnode as (
    select * from {{ ref('stg_prodview__unit_nodes') }}
    where DELETED = false
)

select
    -- Key IDs
    IDREC as NODEID,
    IDRECPARENT as UNITID,
    
    -- Node configuration
    NAME,
    TYP,
    DTTMSTART,
    DTTMEND,
    
    -- Component and fluid properties
    COMPONENT,
    COMPONENTPHASE,
    DESFLUID,
    KEEPWHOLE,
    TYPFLUIDBASERESTRICT,
    
    -- Flow diagram configuration
    SORTFLOWDIAG,
    
    -- Correction and external IDs
    CORRECTIONID1,
    CORRECTIONID2,
    CORRECTIONTYP1,
    CORRECTIONTYP2,
    OTHERID,
    
    -- Product and facility information
    FACPRODUCTNAME,
    USEVIRUTALANALYSIS,
    
    -- Disposition configuration
    DISPOSITIONPOINT,
    DISPPRODUCTNAME,
    TYPDISP1,
    TYPDISP2,
    TYPDISPHCLIQ,
    DISPIDA,
    DISPIDB,
    
    -- Purchaser information
    PURCHASERNAME,
    PURCHASERCODE1,
    PURCHASERCODE2,
    
    -- General configuration
    COM,
    
    -- User-defined fields
    USERTXT1,
    USERTXT2,
    USERTXT3,
    USERNUM1,
    USERNUM2,
    USERNUM3,
    
    -- System audit fields
    SYSCREATEDATE,
    SYSCREATEUSER,
    SYSMODDATE,
    SYSMODUSER,
    
    -- Update tracking
    UPDATE_DATE

from pvunitnode