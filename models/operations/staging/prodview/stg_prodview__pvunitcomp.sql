with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMP') }}
),

renamed as (
    select
        -- Primary identifiers
        IDREC,
        IDRECPARENT,
        IDFLOWNET,
        
        -- Completion identifiers
        COMPIDA,
        COMPIDB,
        COMPIDC,
        COMPIDD,
        COMPLETIONIDE,
        COMPIDPA,
        COMPLETIONCODE,
        COMPIDREGULATORY,
        COMPLETIONNAME,
        COMPLETIONLICENSEE,
        COMPLETIONLICENSENO,
        
        -- Well identifiers
        WELLIDA,
        WELLIDB,
        WELLIDC,
        WELLIDD,
        WELLIDE,
        WELLIDPA,
        WELLIDREGULATORY,
        WELLLICENSENO,
        WELLNAME,
        
        -- Operational dates
        DTTMSTARTALLOC,
        DTTMEND,
        DTTMLASTPRODUCEDCALC,
        DTTMLASTPRODUCEDHCLIQCALC,
        DTTMLASTPRODUCEDGASCALC,
        DTTMFIRSTSALE,
        DTTMFLOWBACKEND,
        DTTMFLOWBACKSTART,
        DTTMABANDON,
        DTTMONPROD,
        DTTMLICENSE,
        
        -- Geographic coordinates
        LATITUDE,
        LONGITUDE,
        LATLONGSOURCE,
        LATLONGDATUM,
        
        -- Production thresholds and parameters
        HELDBYPRODUCTIONTHRESHOLD,
        ENTRYREQPERIODFLUIDLEVEL,
        ENTRYREQPERIODPARAM,
        
        -- Export/Import tracking
        EXPORTID1,
        EXPORTID2,
        EXPORTTYP1,
        EXPORTTYP2,
        IMPORTID1,
        IMPORTID2,
        IMPORTTYP1,
        IMPORTTYP2,
        
        -- Migration tracking
        KEYMIGRATIONSOURCE,
        TYPMIGRATIONSOURCE,
        
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
        
        -- System audit fields
        SYSCREATEDATE,
        SYSCREATEUSER,
        SYSMODDATE,
        SYSMODUSER,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed