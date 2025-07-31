{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'wells', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as "Completion Record ID",
        idrecparent as "Completion Parent Record ID",
        idflownet as "Flow Net ID",
        
        -- Completion lifecycle dates
        dttmstartalloc as "Start Allocating Date in ProdView",
        dttmend as "Expiry Date",
        dttmonprod as "POP Date",
        dttmfirstsale as "First Sale Date",
        dttmflowbackstart as "Flowback Start Date",
        dttmflowbackend as "Flowback End Date",
        dttmabandon as "Abandon Date",
        dttmlastproducedcalc as "Last Produced Date",
        dttmlastproducedhcliqcalc as "Last Produced Oil Date",
        dttmlastproducedgascalc as "Last Produced Gas Date",
        
        -- Production threshold
        heldbyproductionthreshold as "Held by Production Threshold",
        
        -- Completion identifiers
        completionname as "Completion Name",
        permanentid as "Permanent Completion ID",
        compidregulatory as "GHG Report Basin",
        compidpa as "Production Accounting Identifier for Completion",
        completionlicensee as "Completion Licensee",
        completionlicenseno as "Federal Lease #",
        dttmlicense as "Completion License Date",
        compida as "Well Number",
        compidb as "Gas POP ID",
        compidc as "Gas Meter No.",
        compidd as "Gas Alloc Meter No.",
        completionide as "Gas Alloc Group No.",
        completioncode as "Surface Commingle Number",
        
        -- Well identifiers
        wellname as "Well Name",
        wellidregulatory as "Regulatory ID of Well",
        wellidpa as "Production Accounting Identifier of Well",
        welllicenseno as "LA - Serial #, ND - Well File #, TX-MS-OK- API 14",
        wellida as "API 10",
        wellidb as "Cost Center",
        wellidc as "EID",
        wellidd as "Producing Formation",
        wellide as "Legal Well Name",
        
        -- Import/Export tracking
        importid1 as "Import ID 1",
        importtyp1 as "Import Type 1",
        importid2 as "Import ID 2",
        importtyp2 as "Import Type 2",
        exportid1 as "Export ID 1",
        exporttyp1 as "Export Type 1",
        exportid2 as "Export ID 2",
        exporttyp2 as "Export Type 2",
        
        -- Location information
        latitude as "Bottomhole Latitude",
        longitude as "Bottomhole Longitude",
        latlongsource as "Lat/Long Data Source",
        latlongdatum as "Lat/Long Datum",
        
        -- Entry requirements
        entryreqperiodfluidlevel as "Entry Requirement Period Fluid Level",
        entryreqperiodparam as "Entry Requirement Period Parameters",
        
        -- User-defined fields - Text
        usertxt1 as "BHA Type PAGA/SAGA/RPGA",
        usertxt2 as "RESCAT",
        usertxt3 as "Electric Vendor Name",
        usertxt4 as "Electric Meter Name",
        usertxt5 as "Working Interest Partner",
        
        -- User-defined fields - Numeric
        usernum1 as "Surface Casing",
        usernum2 as "Prod Casing",
        usernum3 as "Prod Liner",
        usernum4 as "Purchaser CTB Lease ID",
        usernum5 as "Purchaser Well Lease ID",
        
        -- User-defined fields - Datetime
        userdttm1 as "Spud Date",
        userdttm2 as "userdttm2",
        userdttm3 as "Rig Release Date",
        userdttm4 as "User Date 4",
        userdttm5 as "User Date 5",
        
        -- Migration and integration
        keymigrationsource as "Migration Source Key",
        typmigrationsource as "Migration Source Type",
        
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
--where not "Spud Date" = null