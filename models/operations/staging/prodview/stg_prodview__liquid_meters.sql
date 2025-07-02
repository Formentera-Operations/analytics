{{
  config(
    materialized='view',
    alias='pvunitmeterliquid'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUID') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        
        -- Meter configuration
        NAME,
        ENTRYSOURCE,
        TYP,
        TYPRECORDING,
        UOMVOL,
        PRODUCTNAME,
        
        -- Meter settings and calibration
        REZEROSTART,
        READINGROLLOVER,
        ESTMISSINGDAY,
        
        -- Initial BSW (Basic Sediment and Water) with unit conversion (decimal to percentage)
        INITIALBSW / 0.01 as INITIALBSW,
        case when INITIALBSW is not null then '%' else null end as INITIALBSWUNITLABEL,
        
        -- Identification numbers
        SERIALNUM,
        ENGINEERINGID,
        REGULATORYID,
        OTHERID,
        
        -- Location and references
        LOCPROVTAP,
        IDRECUNITNODECALC,
        IDRECUNITNODECALCTK,
        IDRECUNITDATAENTRYOR,
        IDRECUNITDATAENTRYORTK,
        
        -- Import/Export tracking
        IMPORTID1,
        IMPORTTYP1,
        IMPORTID2,
        IMPORTTYP2,
        EXPORTID1,
        EXPORTTYP1,
        EXPORTID2,
        EXPORTTYP2,
        
        -- Operational settings
        ENTRYREQPERIOD,
        DTTMHIDE,
        
        -- Migration tracking
        KEYMIGRATIONSOURCE,
        TYPMIGRATIONSOURCE,
        
        -- User-defined fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        USERNUM1,
        USERNUM2,
        USERNUM3,
        
        -- System fields
        SYSSEQ,
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