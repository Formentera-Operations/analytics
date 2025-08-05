{{ config(
    materialized='view',
    tags=['wellview', 'well-header', 'master-data', 'staging']
) }}

with source_data as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEADER') }}
    where _fivetran_deleted = false
),

renamed as (
        select
        -- Primary identifier
        idwell as "Well ID",

        -- Well identifiers
        wellname as "Well Name",
        wellida as "API 10 Number",
        wellidb as "Cost Center",
        wellidc as "EID",
        wellidd as "Producing Formation",
        wellide as "Producing Method",
        lease as "Lease Name",
        leasecode as "State Lease ID",
        padname as "Pad Name",
        padcode as "Facility Name",

        -- Well license information
        operator as "Operator Name",
        operatorcode as "Operated Descriptor",
        welllicenseno as "Unique Well Identifier",
        welllicensee as "Licensee",
        govauthority as "Government Authority",

        -- Well classification
        basin as "Basin Name",
        basincode as "Basin Code",
        fieldname as "Field Name",
        fieldcode as "Regulatory Field Name",
        welltyp1 as "Well Type",
        welltyp2 as "Well Subtype",
        currentwelltyp1calc as "Current Well Type",
        currentwelltyp2calc as "Current Well Subtype",
        primaryfluiddes as "Fluid Type",
        currentprimaryfluiddescalc as "Current Fluid Type",
        wellconfig as "Well Configuration Type",
        wellborenocalc as "Number Of Wellbores",
        currentwellstatus1 as "Well Status",
        currentwellstatus2 as "Well Sub Status",
        currentwellstatus1calc as "Current Well Status",
        currentwellstatus2calc as "Current Well Sub Status",
        dttmstatuscalc as "Current Status Date",
        wellclass as "Sour Class",
        environmentsensitive as "Is Environment Sensitive",
        hih2s as "Is High H2s",
        hisitp as "Is High Sitp",
        locationsensitive as "Is Location Sensitive",
        riskclass as "Risk Class",

        -- Elevations (converted to US units)
        elvorigkb / 0.3048 as "Original Kb Elevation ft",
        elvground / 0.3048 as "Ground Elevation ft",
        elvcasflange / 0.3048 as "Casing Flange Elevation ft",
        elvtubhead / 0.3048 as "Tubing Head Elevation ft",
        elvmudline / 0.3048 as "Mud Line Elevation ft",
        elvlat / 0.3048 as "Lowest Astronomical Tide Elevation ft",
        idrecelvhistorycalc as "Active Working Elevation ID",
        idrecelvhistorycalctk as "Active Working Elevation Table Key",

        -- Elevation differences (converted to US units)
        kbtotubcalc / 0.3048 as "Kb To Tubing Head Distance ft",
        kbtocascalc / 0.3048 as "Kb To Casing Flange Distance ft",
        kbtogrdcalc / 0.3048 as "Kb To Ground Distance ft",
        kbtomudcalc / 0.3048 as "Kb To Mud Line Distance ft",
        kbtoothercalc / 0.3048 as "Kb To Other Distance ft",
        othertotubcalc / 0.3048 as "Other To Tubing Head Distance ft",
        othertocascalc / 0.3048 as "Other To Casing Flange Distance ft",
        othertogrdcalc / 0.3048 as "Other To Ground Distance ft",
        othertomudcalc / 0.3048 as "Other To Mud Line Distance ft",

        -- Depths (converted to US units)
        waterdepthref as "Water Depth Reference",
        waterdepth / 0.3048 as "Water Depth ft",
        tdcalc / 0.3048 as "Total Depth ft",
        tdallcalc as "Total Depth All",
        tdtvdallcalc as "Total Depth All Tvd",
        pbtdallcalc as "Pbtd All",
        displaceunwrapcalc / 0.3048 as "Unwrapped Displacement ft",

        -- Important dates
        dttmwelllic as "Permit Date",
        dttmspud as "Spud Date",
        dttmrr as "Rig Release Date",
        dttmfirstprodest as "Estimated On Production Date",
        dttmfirstprod as "On Production Date",
        dttmzoneonprodfirstcalc as "First Zone On Production Date",
        dttmlastprodest as "Estimated Last Production Date",
        dttmlastprod as "Last Production Date",
        dttmabandonest as "Estimated Abandonment Date",
        dttmabandon as "Abandon Date",
        dttmwellclass as "H2s Certificate Approval Date",
        durspudtotodaycalc as "Age Of Well Days",

        -- Location information
        locationtyp as "Onshore Offshore Designation",
        legalsurveytyp as "Legal Survey Type",
        legalsurveysubtyp as "Legal Survey Subtype",
        locationnote as "JOA Name",
        legalsurveyloc as "Surface Legal Location",
        nsdist / 0.3048 as "North South Distance ft",
        nsflag as "North South Reference",
        ewdist / 0.3048 as "East West Distance ft",
        ewflag as "East West Reference",
        townname as "Nearest Town",
        towndist / 1609.344 as "Distance To Nearest Town Miles",
        townflag as "Nearest Town Ref Direction",
        townstateprov as "Nearest Town State Prov",
        locationref as "Location Reference",
        area as "Asset Company",
        county as "County Parish",
        stateprov as "State Province",
        country as "Country",
        fieldoffice as "Field Office",
        fieldofficecode as "District Office",
        district as "District",
        division as "Division",
        divisioncode as "Company Code",
        directionstowell as "Directions To Well",

        -- Geographic coordinates
        latlongsource as "Lat/Long Data Source",
        latlongdatum as "Lat/Long Datum",
        latitude as "Latitude Degrees",
        longitude as "Longitude Degrees",
        utmsource as "UTM Source",
        utmgridzone as "UTM Grid Zone",
        utmx as "UTM Easting Meters",
        utmy as "UTM Northing Meters",

        -- Other operational information
        platform as "Route",
        slot as "DSU",
        idrecprodsettingcalc as "Production Setting ID",
        idrecprodsettingcalctk as "Production Setting Table Key",
        casingruncalc as "Number Of Casing Strings",
        problemflag as "Has Problem With Well",
        idrecproblemcalc as "Production Failure ID",
        idrecproblemcalctk as "Production Failure Table Key",
        problemtotalcalc as "Total Number Of Failures",
        problemlast12monthcalc as "Failures In Last 12 Months",
        operated as "Is Operated",
        surfacerights as "Surface Rights",
        agent as "Agent",
        localtimezone / 0.0416666666666667 as "Local Time Zone Hours",
        lastjobcalc as "Last Job",
        lastjobreportcalc as "Last Daily Ops Report",

        -- User fields
        usertxt1 as "RTP Lease Clause",
        usertxt2 as "Election Days",
        usertxt3 as "Payout Penalty Percent",
        usertxt4 as "Order Number",
        usertxt5 as "Mawp Psig",
        usertxt6 as "Mawp Weak Point Description",
        usertxt7 as "Maasp Psig",
        usertxt8 as "UIC Permit Pressure Psig",
        usertxt9 as "UIC Permit Rate Bpd",
        usertxt10 as "Acquisition Accounting ID",
        userboolean1 as "Is Last Well On Lease",
        userboolean2 as "Is BLM",
        userboolean3 as "Is Fed Surface",
        userboolean4 as "Is Split Estate",
        userboolean5 as "Is FO Drilled",
        usernum1 as "Working Interest",
        usernum2 as "NRI Total",
        usernum3 as "NRI Wi Only",
        usernum4 as "Override Decimal",
        usernum5 as "Mineral Royalty Decimal",
        usernum6 as "Shut In Clock Days",
        userdttm1 as "First Sales Date",
        userdttm2 as "Ops Effective Date",
        userdttm3 as "Regulatory Effective Date",
        userdttm4 as "Last Approved MIT Date",
        userdttm5 as "JOA Date",

        -- Comments
        com as "Land Legal Description",

        -- System fields
        syscreatedate as "Created At (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod At (UTC)",
        sysmoduser as "Last Mod By",
        systag as "System Tag",
        sysmoddatedb as "Last Write To Database",
        sysmoduserdb as "Last Write To Database User",
        syssecuritytyp as "Security Type",
        syslockdatemaster as "Master Lock Date",
        syslockmeui as "System Lock Me UI",
        syslockchildrenui as "System Lock Children UI",
        syslockme as "System Lock Me",
        syslockchildren as "System Lock Children",
        syslockdate as "System Lock Date",

        -- Fivetran metadata
        _fivetran_synced as "Fivetran Synced At"


    from source_data
)

select * from renamed