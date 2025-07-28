{{
  config(
    full_refresh=false,
    materialized='view',
    alias='unit_v2'
  )
}}

with pvunit as (
    select * from {{ ref('stg_prodview__units') }}
),

pvroutesetroute as (
    select * from {{ ref('stg_prodview__routes') }}
),

pvunitcomp as (
    select * from {{ ref('stg_prodview__completions') }}
),

svintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where "Product Description" = 'SiteView' 
    and "Table Key" = 'pvunit'
),

wvcompintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where "Product Description" = 'WellView' 
    and "Table Key" = 'pvunitcomp'
),

wvintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where "Product Description" = 'WellView' 
    and "Table Key" = 'pvunit'
)

select
    -- Route information
    r."Route Name",
    r."Foreman",
    r."Primary Lease Operator",
    r."Backup Lease Operator",

    -- Unit information
    u."API 10",
    u."AssetCo",
    u."BHA Change 1",
    u."BHA Change 2",
    u."Chemical Provider",
    u."Combo Curve ID",
    u."Comimingle Permit No.",
    u."Comment",
    u."Company Code",
    u."Cost Center",
    u."Country",
    u."Create Date (UTC)",
    u."Current Facility",
    u."DSU",
    u."District",
    u."District Office",
    u."Display Name",
    u."Electric Acct. No.",
    u."Electric Allocation Meter Number",
    u."Electric Meter ID",
    u."Electric Vendor No.",
    u."EID",
    u."Facility Name",
    u."Field Office",
    u."Foreman Area",
    u."Gas Gathering System Name",
    u."Gas Purchaser",
    u."Government Authority",
    u."Ground Elevation",
    u."HCLiq Disposition Method",
    u."HCLiq Inventory Type",
    u."Hide Record As Of",
    u."Is Cycled",
    u."Is Operated",
    u."Is Purchaser",
    u."Last Mod By",
    u."Last Mod Date (UTC)",
    u."Lat/Long Datum",
    u."LatLong Source",
    u."Lease Name",
    u."Migration Source Key",
    u."Migration Source Type",
    u."Operated Descriptor",
    u."Operator",
    u."Pad Name",
    u."Parent Record ID",
    u."Route",
    u."Producing Method",
    u."Property Number",
    u."Record ID",
    u."Regulatory Field Name",
    u."Regulatory ID",
    u."Regulatory Unit Type",
    u."Route",
    u."Start Displaying Unit On",
    u."State/Province",
    u."Stop Displaying Unit After",
    u."Stripper Date",
    u."Stripper Type",
    u."Surface Latitude",
    u."Surface Legal Location",
    u."Surface Longitude",
    u."SWD System",
    u."TX - Lease Number, LA - LUW, ND-MS-OK - N/A",
    u."Unit Name",
    u."Unit Sub Type",
    u."Unit Type",
    u."User Date 4",
    u."User Date 5",
    u."User Num 5",
    u."UserTxt1",
    u."UTM Easting",
    u."UTM Grid Zone",
    u."UTM Northing",


    -- Completion information
    c."API10",
    c."Abandon Date",
    c."BHA Type PAGA/SAGA/RPGA",
    c."Bottomhole Latitude",
    c."Bottomhole Longitude",
    c."Completion License Date",
    c."Completion Licensee",
    c."Completion Name",
    c."CostCenter",
    c."Create Date (UTC)",
    c."Created By",
    c."EID",
    c."Electric Meter Name",
    c."Electric Vendor Name",
    c."Entry Requirement Period Fluid Level",
    c."Entry Requirement Period Parameters",
    c."Expiry Date",
    c."Export ID 1",
    c."Export ID 2",
    c."Export Type 1",
    c."Export Type 2",
    c."Federal Lease #",
    c."First Sale Date",
    c."Flowback End Date",
    c."Flowback Start Date",
    c."GHG Report Basin",
    c."Gas Alloc Group No.",
    c."Gas Alloc Meter No.",
    c."Gas Meter No.",
    c."Gas POP ID",
    c."Held by Production Threshold",
    c."Import ID 1",
    c."Import ID 2",
    c."Import Type 1",
    c."Import Type 2",
    c."LA - Serial #, ND - Well File #, TX-MS-OK- API 14",
    c."Last Mod By",
    c."Last Mod Date (UTC)",
    c."Last Produced Date",
    c."Last Produced Gas Date",
    c."Last Produced Oil Date",
    c."Lat Long Data Source",
    c."Lat/Long Datum",
    c."Legal Well Name",
    c."Migration Source Key",
    c."Migration Source Type",
    c."POP Date",
    c."Prod Casing",
    c."Prod Liner",
    c."Producing Formation",
    c."Production Accounting Identifier for Completion",
    c."Production Accounting Identifier of Well",
    c."Purchaser CTB Lease ID",
    c."Purchaser Well Lease ID",
    c."RESCAT",
    c."Regulatory ID of Well",
    c."Rig Release Date",
    c."Spud Date",
    c."Spud Date",
    c."Start Allocating Date in ProdView",
    c."Surface Casing",
    c."Surface Commingle Number",
    c."User Date 4",
    c."User Date 5",
    c."Well Name",
    c."Well Number",
    c."Working Interest Partner",


    -- Integration IDs
    si."AF ID Rec" as "SiteView Site ID",
    wci."AF ID Rec" as "WellView Completion ID",
    wi."AF ID Rec" as "WellView Well ID",

    -- Update tracking
    greatest(
        coalesce(r."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z'),
        coalesce(u."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z'),
        coalesce(c."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z'),
        coalesce(si."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z'),
        coalesce(wci."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z'),
        coalesce(wi."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z')
    ) as UPDATE_DATE

from pvunit u
left join pvroutesetroute r 
    on u."Current Route" = r."AF ID Rec"
left join pvunitcomp c 
    on u."AF ID Rec" = c."Parent Record ID"
left join svintegration si 
    on u."AF ID Rec"= si."Parent Record ID" 
    and si."Flow Net ID" = u."Flow Net ID"
left join wvcompintegration wci 
    on u."AF ID Rec"= wci."Parent Record ID" 
    and wci."Flow Net ID" = u."Flow Net ID"
left join wvintegration wi 
    on u."AF ID Rec"= wi."Parent Record ID" 
    and wi."Flow Net ID" = u."Flow Net ID"