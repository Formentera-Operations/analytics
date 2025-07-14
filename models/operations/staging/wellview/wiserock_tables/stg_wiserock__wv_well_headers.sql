{{ config(
    materialized='view',
    tags=['wellview', 'well', 'header', 'master', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEADER') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifier
        idwell,
        
        -- Administrative information
        agent,
        area,
        basin,
        basincode,
        casingruncalc,
        com,
        country,
        county,
        
        -- Current status information
        currentprimaryfluiddescalc,
        currentwellstatus1,
        currentwellstatus1calc,
        currentwellstatus2,
        currentwellstatus2calc,
        currentwelltyp1calc,
        currentwelltyp2calc,
        
        -- Location and navigation
        directionstowell,
        
        -- Displacement calculation (converted from meters to feet)
        displaceunwrapcalc / 0.3048 as displaceunwrapcalc,
        
        -- Administrative divisions
        district,
        division,
        divisioncode,
        
        -- Important dates
        dttmabandon,
        dttmabandonest,
        dttmfirstprod,
        dttmfirstprodest,
        dttmlastprod,
        dttmlastprodest,
        dttmrr,
        dttmspud,
        dttmstatuscalc,
        dttmwellclass,
        dttmwelllic,
        dttmzoneonprodfirstcalc,
        
        -- Duration calculations (no conversion - remains as days)
        durspudtotodaycalc,
        
        -- Elevation measurements (converted from meters to feet)
        elvcasflange / 0.3048 as elvcasflange,
        elvground / 0.3048 as elvground,
        elvlat / 0.3048 as elvlat,
        elvmudline / 0.3048 as elvmudline,
        elvorigkb / 0.3048 as elvorigkb,
        elvtubhead / 0.3048 as elvtubhead,
        
        -- Environmental and operational flags
        environmentsensitive,
        
        -- Distance measurements (converted from meters to feet)
        ewdist / 0.3048 as ewdist,
        ewflag,
        
        -- Field information
        fieldcode,
        fieldname,
        fieldoffice,
        fieldofficecode,
        
        -- Regulatory and safety
        govauthority,
        hih2s,
        hisitp,
        
        -- Reference IDs
        idrecelvhistorycalc,
        idrecelvhistorycalctk,
        idrecproblemcalc,
        idrecproblemcalctk,
        idrecprodsettingcalc,
        idrecprodsettingcalctk,
        
        -- KB to elevation calculations (converted from meters to feet)
        kbtocascalc / 0.3048 as kbtocascalc,
        kbtogrdcalc / 0.3048 as kbtogrdcalc,
        kbtomudcalc / 0.3048 as kbtomudcalc,
        kbtoothercalc / 0.3048 as kbtoothercalc,
        kbtotubcalc / 0.3048 as kbtotubcalc,
        
        -- Last job information
        lastjobcalc,
        lastjobreportcalc,
        
        -- Geographic coordinates
        latitude,
        latlongdatum,
        latlongsource,
        
        -- Lease information
        lease,
        leasecode,
        
        -- Legal survey information
        legalsurveyloc,
        legalsurveysubtyp,
        legalsurveytyp,
        
        -- Local timezone (converted from days to hours)
        localtimezone / 0.0416666666666667 as localtimezone,
        
        -- Location details
        locationnote,
        locationref,
        locationsensitive,
        locationtyp,
        longitude,
        
        -- North-South distance (converted from meters to feet)
        nsdist / 0.3048 as nsdist,
        nsflag,
        
        -- Operator information
        operated,
        operator,
        operatorcode,
        
        -- Other to elevation calculations (converted from meters to feet)
        othertocascalc / 0.3048 as othertocascalc,
        othertogrdcalc / 0.3048 as othertogrdcalc,
        othertomudcalc / 0.3048 as othertomudcalc,
        othertotubcalc / 0.3048 as othertotubcalc,
        
        -- Pad information
        padcode,
        padname,
        pbtdallcalc,
        platform,
        
        -- Production information
        primaryfluiddes,
        
        -- Problem tracking
        problemflag,
        problemlast12monthcalc,
        problemtotalcalc,
        
        -- Risk and operational details
        riskclass,
        slot,
        stateprov,
        surfacerights,
        
        -- Total depth information
        tdallcalc,
        tdcalc / 0.3048 as tdcalc,
        tdtvdallcalc,
        
        -- Town information (distance converted from meters to miles)
        towndist / 1609.344 as towndist,
        townflag,
        townname,
        townstateprov,
        
        -- User-defined fields
        userboolean1,
        userboolean2,
        userboolean3,
        userboolean4,
        userboolean5,
        userdttm1,
        userdttm2,
        userdttm3,
        userdttm4,
        userdttm5,
        usernum1,
        usernum2,
        usernum3,
        usernum4,
        usernum5,
        usernum6,
        usertxt1,
        usertxt10,
        usertxt2,
        usertxt3,
        usertxt4,
        usertxt5,
        usertxt6,
        usertxt7,
        usertxt8,
        usertxt9,
        
        -- UTM coordinates (no conversion - remain in meters)
        utmgridzone,
        utmsource,
        utmx,
        utmy,
        
        -- Water depth (converted from meters to feet)
        waterdepth / 0.3048 as waterdepth,
        waterdepthref,
        
        -- Well identification
        wellborenocalc,
        wellclass,
        wellconfig,
        wellida,
        wellidb,
        wellidc,
        wellidd,
        wellide,
        welllicensee,
        welllicenseno,
        wellname,
        welltyp1,
        welltyp2,
        
        -- System fields
        syslockmeui,
        syslockchildrenui,
        syslockme,
        syslockchildren,
        syslockdate,
        sysmoddate,
        sysmoduser,
        syscreatedate,
        syscreateuser,
        systag,
        sysmoddatedb,
        sysmoduserdb,
        syssecuritytyp,
        syslockdatemaster,
        
        -- Fivetran metadata
        _fivetran_synced as updatedate,
        _fivetran_deleted as deleted

    from source_data
)

select * from renamed