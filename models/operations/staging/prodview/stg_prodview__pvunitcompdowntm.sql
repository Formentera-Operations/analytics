{{
  config(
    materialized='view',
    alias='pvunitcompdowntm'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPDOWNTM') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        
        -- Downtime configuration
        TYPDOWNTM,
        DTTMSTART,
        DTTMEND,
        DTTMPLANEND,
        
        -- Duration fields with unit conversion (minutes to hours)
        DURDOWNSTARTDAY / 0.0416666666666667 as DURDOWNSTARTDAY,
        case when DURDOWNSTARTDAY is not null then 'HR' else null end as DURDOWNSTARTDAYUNITLABEL,
        DURDOWNENDDAY / 0.0416666666666667 as DURDOWNENDDAY,
        case when DURDOWNENDDAY is not null then 'HR' else null end as DURDOWNENDDAYUNITLABEL,
        DURDOWNCALC / 0.0416666666666667 as DURDOWNCALC,
        case when DURDOWNCALC is not null then 'HR' else null end as DURDOWNCALCUNITLABEL,
        DURDOWNPLANEND / 0.0416666666666667 as DURDOWNPLANEND,
        case when DURDOWNPLANEND is not null then 'HR' else null end as DURDOWNPLANENDUNITLABEL,
        
        -- Downtime codes
        CODEDOWNTM1,
        CODEDOWNTM2,
        CODEDOWNTM3,
        
        -- Additional information
        COM,
        LOCATION,
        FAILFLAG,
        PRODUCT,
        
        -- User-defined text fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        USERTXT4,
        USERTXT5,
        
        -- User-defined numeric fields
        USERNUM1,
        USERNUM2,
        USERNUM3,
        USERNUM4,
        USERNUM5,
        
        -- User-defined datetime fields
        USERDTTM1,
        USERDTTM2,
        USERDTTM3,
        USERDTTM4,
        USERDTTM5,
        
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