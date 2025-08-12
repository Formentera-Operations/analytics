{{ config(
    materialized='view',
    tags=['prodview', 'units', 'facilities', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNIT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Unit Record ID",
        idflownet as "Flow Net ID",
        name as "Unit Name",
        nameshort as "Unit Display Name",
        
        -- Unit classification
        typ1 as "Unit Type",
        typ2 as "Unit Sub Type",
        typregulatory as "Regulatory Unit Type",
        typdisphcliq as "HCLiq Inventory Type",
        typdispngl as "NGL Inventory Type",
        dispproductname as "HCLiq Disposition Method",
        typpa as "Comimingle Permit No.",
        
        -- Status and operational info
        priority as "Is Cycled",
        operated as "Is Operated",
        operator as "Operator",
        operatorida as "Operated Descriptor",
        purchaser as "Is Purchaser",
        displaysizefactor as "Display Size Factor",
        
        -- Temporal information
        dttmstart as "Start Displaying Unit On",
        dttmend as "Stop Displaying Unit After",
        dttmhide as "Hide Record As Of",
        
        -- Location - Geographic coordinates
        latitude as "Surface Latitude",
        longitude as "Surface Longitude",
        latlongsource as "Lat/Long Data Source",
        latlongdatum as "Lat/Long Datum",
        
        -- Location - UTM coordinates (keeping in meters)
        utmgridzone as "UTM Grid Zone",
        utmx as "UTM Easting",
        utmy as "UTM Northing",
        utmsource as "Location",
        
        -- Location - Physical/Administrative
        elevation / 0.3048 as "Ground Elevation",
        legalsurfloc as "Surface Legal Location",
        division as "Division",
        divisioncode as "Company Code",
        district as "District",
        area as "AssetCo",
        field as "Foreman Area",
        fieldcode as "Regulatory Field Name",
        fieldoffice as "Field Office",
        fieldofficecode as "District Office",
        country as "Country",
        stateprov as "State/Province",
        county as "County",
        
        -- Facility and infrastructure
        platform as "Route",
        padcode as "Facility Name",
        padname as "Pad Name",
        slot as "DSU",
        locationtyp as "SWD System",
        
        -- Business identifiers
        unitidregulatory as "Regulatory ID",
        unitidpa as "EID",
        unitida as "API 10",
        unitidb as "Property Number",
        unitidc as "Combo Curve ID",
        stopname as "Stop Name",
        lease as "Lease Name",
        leaseida as "TX - Lease Number, LA - LUW, ND-MS-OK - N/A",
        
        -- Financial and organizational
        costcenterida as "Cost Center",
        costcenteridb as "Gas Gathering System Name",
        govauthority as "Government Authority",
        
        -- Current status references (calculated fields)
        idrecroutesetroutecalc as "Current Route",
        idrecroutesetroutecalctk as "Current Route Table",
        idrecfacilitycalc as "Current Facility",
        idrecfacilitycalctk as "Current Facility Table",
        idreccompstatuscalc as "Current Completion Status",
        idreccompstatuscalctk as "Current Completion Status Table",
        
        -- Responsible parties
        idrecresp1 as "Oil Purchaser",
        idrecresp1tk as "Primary Responsible Table",
        idrecresp2 as "Gas Purchaser",
        idrecresp2tk as "Secondary Responsible Table",
        
        -- Migration and integration
        keymigrationsource as "Migration Source Key",
        typmigrationsource as "Migration Source Type",
        
        -- User-defined fields - Text
        usertxt1 as "UserTxt1",
        usertxt2 as "Completion Status",
        usertxt3 as "Producing Method",
        usertxt4 as "Stripper Type",
        usertxt5 as "Chemical Provider",
        
        -- User-defined fields - Numeric
        usernum1 as "Electric Allocation Meter Number",
        usernum2 as "Electric Meter ID",
        usernum3 as "Electric Acct. No.",
        usernum4 as "Electric Vendor No.",
        usernum5 as "User Num 5",
        
        -- User-defined fields - Datetime
        userdttm1 as "Stripper Date",
        userdttm2 as "BHA Change 1",
        userdttm3 as "BHA Change 2",
        userdttm4 as "User Date 4",
        userdttm5 as "User Date 5",
        
        -- Administrative
        sortbyuser as "Unit Sort",
        timezone as timezone,
        com as "Comment",
        
        -- System fields
        syscreatedate as "Create Date (UTC)",
        syscreateuser as "Created By",
        sysmoddate as "Last Mod Date (UTC)",
        sysmoduser as "Last Mod By",
        systag as "Record Tag",
        syslockdate as "Lock Date (UTC)",
        syslockme as "Lock Me",
        syslockchildren as "Lock My Children",
        syslockmeui as "Lock Me (UI)",
        syslockchildrenui as "Lock My Children (UI)",
        
        -- Fivetran fields
        _fivetran_synced as "Fivetran Synced At"
        
    from source_data
)

select * from renamed