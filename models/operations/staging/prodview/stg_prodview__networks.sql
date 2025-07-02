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
        IDFLOWNET,
        NAME,
        TYP,
        
        -- Primary unit and facility references
        IDRECUNITPRIMARY,
        IDRECUNITPRIMARYTK,
        IDRECFACILITYPRIMARY,
        IDRECFACILITYPRIMARYTK,
        
        -- General information
        COM,
        
        -- Responsibility assignments
        IDRECRESP1,
        IDRECRESP1TK,
        IDRECRESP2,
        IDRECRESP2TK,
        
        -- Reporting configuration flags
        RPTGATHEREDCALCS,
        RPTALLOCATIONS,
        RPTDISPOSITIONS,
        RPTCOMPONENTDISPOSITIONS,
        RPTNODECALCULATIONS,
        
        -- Operational settings
        TRACKDOWNHOLEINVENTORY,
        
        -- Allocation and process dates
        DTTMALLOCPROCESSBEGAN,
        DTTMSTART,
        DTTMEND,
        DTTMLASTALLOCPROCESS,
        USERLASTALLOCPROCESS,
        
        -- User-defined text fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        USERTXT4,
        USERTXT5,
        
        -- System locking fields
        SYSLOCKMEUI,
        SYSLOCKCHILDRENUI,
        SYSLOCKME,
        SYSLOCKCHILDREN,
        SYSLOCKDATE,
        
        -- System audit fields
        SYSMODDATE,
        SYSMODUSER,
        SYSCREATEDATE,
        SYSCREATEUSER,
        SYSTAG,
        
        -- Additional system fields
        SYSMODDATEDB,
        SYSMODUSERDB,
        SYSSECURITYTYP,
        SYSLOCKDATEMASTER,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed