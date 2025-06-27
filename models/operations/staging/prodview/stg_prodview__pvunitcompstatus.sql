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
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        DTTM,
        
        -- Status and operational configuration
        STATUS,
        PRIMARYFLUIDTYP,
        FLOWDIRECTION,
        COMMINGLED,
        TYPFLUIDPROD,
        TYPCOMPLETION,
        METHODPROD,
        
        -- Calculation settings
        CALCLOSTPROD,
        WELLCOUNTINCL,
        
        -- User-defined text fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        
        -- User-defined numeric fields
        USERNUM1,
        USERNUM2,
        USERNUM3,
        
        -- General information
        COM,
        
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
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed