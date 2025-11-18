{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNIT') }}
    qualify 1 = row_number() over (partition by idrec order by _fivetran_synced desc)

),

renamed as (

    select
        -- Primary identifiers
        idflownet,
        idrec,
        
        -- Unit information
        name,
        nameshort,
        typ1,
        typ2,
        typdisphcliq,
        typdispngl,
        dispproductname,
        
        -- Calculated reference fields
        idrecroutesetroutecalc,
        idrecroutesetroutecalctk,
        idrecfacilitycalc,
        idrecfacilitycalctk,
        idreccompstatuscalc,
        idreccompstatuscalctk,
        
        -- Regulatory and display
        typregulatory,
        typpa,
        displaysizefactor,
        
        -- Date fields
        dttmstart,
        dttmend,
        dttmhide,
        
        -- Location coordinates with imperial conversion
        elevation / 0.3048 as elevation,  -- Convert meters to feet
        
        -- Unit identifiers
        unitidregulatory,
        unitidpa,
        stopname,
        unitida,
        unitidb,
        unitidc,
        
        -- Operational details
        purchaser,
        operated,
        operator,
        operatorida,
        com,
        
        -- Location details
        legalsurfloc,
        division,
        divisioncode,
        district,
        country,
        area,
        field,
        fieldcode,
        fieldoffice,
        fieldofficecode,
        stateprov,
        county,
        
        -- Geographic coordinates
        latitude,
        longitude,
        latlongsource,
        latlongdatum,
        utmgridzone,
        utmsource,
        utmx,  -- Keep in meters
        utmy,  -- Keep in meters
        
        -- Lease and facility information
        lease,
        leaseida,
        locationtyp,
        platform,
        padcode,
        padname,
        slot,
        
        -- Administrative
        govauthority,
        costcenterida,
        costcenteridb,
        sortbyuser,
        priority,
        timezone,
        
        -- Responsible parties
        idrecresp1,
        idrecresp1tk,
        idrecresp2,
        idrecresp2tk,
        
        -- Migration tracking
        keymigrationsource,
        typmigrationsource,
        
        -- User-defined fields
        usertxt1,
        usertxt2,
        usertxt3,
        usertxt4,
        usertxt5,
        usernum1,
        usernum2,
        usernum3,
        usernum4,
        usernum5,
        userdttm1,
        userdttm2,
        userdttm3,
        userdttm4,
        userdttm5,
        
        -- System lock fields
        syslockmeui,
        syslockchildrenui,
        syslockme,
        syslockchildren,
        syslockdate,
        
        -- System metadata
        sysmoddate,
        sysmoduser,
        syscreatedate,
        syscreateuser,
        systag,
        
        -- Fivetran metadata mapped to standard names
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source

)

select * from renamed