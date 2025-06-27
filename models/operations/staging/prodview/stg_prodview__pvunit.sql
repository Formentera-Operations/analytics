with source as (
    select * from {{ source('prodview', 'PVT_PVUNIT') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDREC,
        NAME,
        NAMESHORT,
        
        -- Unit types and classifications
        TYP1,
        TYP2,
        TYPDISPHCLIQ,
        TYPDISPNGL,
        DISPPRODUCTNAME,
        TYPREGULATORY,
        TYPPA,
        
        -- Calculated references
        IDRECROUTESETROUTECALC,
        IDRECROUTESETROUTECALCTK,
        IDRECFACILITYCALC,
        IDRECFACILITYCALCTK,
        IDRECCOMPSTATUSCALC,
        IDRECCOMPSTATUSCALCTK,
        
        -- Display and UI settings
        DISPLAYSIZEFACTOR,
        
        -- Operational dates
        DTTMSTART,
        DTTMEND,
        DTTMHIDE,
        
        -- Elevation with unit conversion (meters to feet)
        ELEVATION / 0.3048 as ELEVATION,
        case 
            when ELEVATION is not null then 'FT'
            else null 
        end as ELEVATIONUNITLABEL,
        
        -- Regulatory and PA identifiers
        UNITIDREGULATORY,
        UNITIDPA,
        STOPNAME,
        UNITIDA,
        UNITIDB,
        UNITIDC,
        
        -- Operational information
        PURCHASER,
        OPERATED,
        OPERATOR,
        OPERATORIDA,
        COM,
        LEGALSURFLOC,
        
        -- Geographic hierarchy
        DIVISION,
        DIVISIONCODE,
        DISTRICT,
        COUNTRY,
        AREA,
        FIELD,
        FIELDCODE,
        FIELDOFFICE,
        FIELDOFFICECODE,
        STATEPROV,
        COUNTY,
        
        -- Geographic coordinates
        LATITUDE,
        LONGITUDE,
        LATLONGSOURCE,
        LATLONGDATUM,
        
        -- UTM coordinates with unit labels
        UTMGRIDZONE,
        UTMSOURCE,
        UTMX,
        case 
            when UTMX is not null then 'M'
            else null 
        end as UTMXUNITLABEL,
        UTMY,
        case 
            when UTMY is not null then 'M'
            else null 
        end as UTMYUNITLABEL,
        
        -- Location details
        LEASE,
        LEASEIDA,
        LOCATIONTYP,
        PLATFORM,
        PADCODE,
        PADNAME,
        SLOT,
        GOVAUTHORITY,
        
        -- Business information
        COSTCENTERIDA,
        COSTCENTERIDB,
        SORTBYUSER,
        PRIORITY,
        TIMEZONE,
        
        -- Responsibility assignments
        IDRECRESP1,
        IDRECRESP1TK,
        IDRECRESP2,
        IDRECRESP2TK,
        
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