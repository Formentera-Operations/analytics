

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODE') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        
        -- Node configuration
        NAME,
        TYP,
        DTTMSTART,
        DTTMEND,
        
        -- Fluid and component properties
        COMPONENT,
        COMPONENTPHASE,
        DESFLUID,
        KEEPWHOLE,
        TYPFLUIDBASERESTRICT,
        
        -- Flow diagram and sorting
        SORTFLOWDIAG,
        
        -- Migration tracking
        KEYMIGRATIONSOURCE,
        TYPMIGRATIONSOURCE,
        
        -- External IDs and corrections
        OTHERID,
        CORRECTIONID1,
        CORRECTIONTYP1,
        CORRECTIONID2,
        CORRECTIONTYP2,
        
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
        DTTMHIDE,
        REPORTGROUP,
        INGATHERED,
        
        -- User-defined fields
        USERTXT1,
        USERTXT2,
        USERTXT3,
        USERNUM1,
        USERNUM2,
        USERNUM3,
        
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