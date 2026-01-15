{{
    config(
        enabled=true,
        materialized='view'
    )
}}

with prodview as (
    select *
    from {{ ref('int_prodview__well_header') }}
),

wellview as (
    select *
    from {{ ref('int_wellview__well_header') }}
),

company as (
    Select *
    from {{ ref('dim_companies') }} 
),

oda as (
    select *
    from {{ ref('int_oda_wells') }}
),

prodstatus as (
    SELECT
        "Unit Record ID"
        ,"Status Record ID"
        ,"Last Mod At (UTC)"
        ,ROW_NUMBER() OVER (
            PARTITION BY "Unit Record ID"
            ORDER BY "Last Mod At (UTC)" DESC
        ) AS rn
    from {{ ref('int_prodview__production_volumes') }}
    --where rn = 1
),

route as (
    select
        *
    from {{ ref('int_dim_route') }}
),

tbl as (
    Select
        w."Abandon Date"
        ,IFNULL(p."API 10", o."ApiNumber") AS "API 10"
        ,case
            when c.company_name is null then 
                (case
                    when lower(p."AssetCo") = 'fp south texas' then 'Formentera South Texas, LP'
                    when lower(p."AssetCo") = 'fp balboa la' then 'FP Balboa LA LLC'
                    when lower(p."AssetCo") = 'fp balboa la midstream' then 'FP Balboa LA Midstream'
                    when lower(p."AssetCo") = 'fp balboa ms' then 'FP Balboa MS LLC'
                    when lower(p."AssetCo") = 'fp balboa nd' then 'FP Balboa ND LLC'
                    when lower(p."AssetCo") = 'fp divide' then 'FP Divide LLC'
                    when lower(p."AssetCo") = 'fp drake' then 'FP Drake LLC'
                    when lower(p."AssetCo") = 'fp goldsmith' then 'FP Goldsmith LP'
                    when lower(p."AssetCo") = 'fp kingfisher' then 'FP Kingfisher LLC'
                    when lower(p."AssetCo") = 'fp lariat' then 'FP Lariat, LLC'
                    when lower(p."AssetCo") = 'fp maverick' then 'FP Maverick LP'
                    when lower(p."AssetCo") = 'fp meramec' then 'FP Meramec LLC'
                    when lower(p."AssetCo") = 'fp overlook' then 'FP Overlook LLC'
                    when lower(p."AssetCo") = 'fp pronghorn' then 'FP Pronghorn LLC'
                    when lower(p."AssetCo") = 'fp wheeler' then 'FP Wheeler Upstream LLC'
                    when lower(p."AssetCo") = 'fp wheeler midstream' then 'FP Wheeler Upstream LLC'
                    when lower(p."AssetCo") = 'fp wheeler upstream' then 'FP Wheeler Upstream LLC'
                    when lower(p."AssetCo") = 'snyder drillco' then 'Snyder Drill Co LP'
                    else p."AssetCo" end)
            when c.company_name is null and p."AssetCo" is null and not w."Asset Company" is null then w."Asset Company"
            when c.company_name is null and p."AssetCo" is null and w."Asset Company" is null then o."CompanyName"
            else c.company_name
        end as "Asset Company"
        ,case
            when c.company_code is null then 
                (case
                    when lower(p."AssetCo") = 'fp south texas' then 810
                    when lower(p."AssetCo") = 'fp balboa la' then 707
                    when lower(p."AssetCo") = 'fp balboa la midstream' then 706
                    when lower(p."AssetCo") = 'fp balboa ms' then 708
                    when lower(p."AssetCo") = 'fp balboa nd' then 709
                    when lower(p."AssetCo") = 'fp divide' then 701
                    when lower(p."AssetCo") = 'fp drake' then 813
                    when lower(p."AssetCo") = 'fp goldsmith' then 807
                    when lower(p."AssetCo") = 'fp lariat' then 811
                    when lower(p."AssetCo") = 'fp kingfisher' then 704
                    when lower(p."AssetCo") = 'fp maverick' then 703
                    when lower(p."AssetCo") = 'fp meramec' then 804
                    when lower(p."AssetCo") = 'fp overlook' then 800
                    when lower(p."AssetCo") = 'fp pronghorn' then 809
                    when lower(p."AssetCo") = 'fp wheeler' then 300
                    when lower(p."AssetCo") = 'fp wheeler midstream' then 300
                    when lower(p."AssetCo") = 'fp wheeler upstream' then 300
                    when lower(p."AssetCo") = 'snyder drillco' then 500
                    else left(p."Property Number", 3) end)
                when c.company_code is null and p."AssetCo" is null and not w."Company Code" is null then w."Company Code"
                when c.company_code is null and p."AssetCo" is null and w."Company Code" is null then o."CompanyCode"
            else c.company_code end
        as "Asset Company Code"
        ,p."Completion Record ID"
        ,case when p."Completion Status" is null then o."WellStatusTypeName" else p."Completion Status" end as "Completion Status"
        ,case when p."District" is null then o."SEARCHKEY" else p."District" end as "Business Unit"
        ,case when p."Unit Create Date (UTC)" is null then o."Created Date" else p."Unit Create Date (UTC)" end as "Unit Create Date (UTC)"
        ,CAST(w."On Production Date" AS date) AS "First Prod Date"
        ,case
            when p."First Sale Date" is null and not w."First Sales Date" is null then CAST(w."First Sales Date" AS date)
            when w."First Sales Date" is null and not p."First Sale Date" is null then CAST(p."First Sale Date" AS date)
            else CAST(w."First Sales Date" AS date)
        end as "First Sales Date"           
        ,p."Current Facility"
        ,p."Facility Name"
        ,r."Foreman"
        ,p."Foreman Area"
        ,case when p."Is Operated" is null then o.OP_IS_OPERATED else p."Is Operated" end as "PV Is Operated"
        ,greatest(
            coalesce(p."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z'),
            coalesce(w."Last Mod At (UTC)", '0000-01-01T00:00:00.000Z'),
            coalesce(o."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z')
            ) as "Last Mod Date (UTC)"
        ,w."Last Approved MIT Date"
        ,w."Last Write To Database"
        ,w."Lat/Long Datum"
        ,w."Latitude Degrees"
        ,w."Longitude Degrees"
        ,o."SEARCHKEY" as "ODA Asset"
        ,o."FIELD" as "ODA Field"
        ,w."UTM Easting Meters"
        ,w."UTM Northing Meters"
        ,w."Master Lock Date"
        ,cast(w."Ops Effective Date" as date) as "Ops Effective Date"
        ,w."Permit Date"
        ,s."Status Record ID" as "Prod Status Record ID"
        ,p."Producing Method"
        ,p."Property EID"
        ,case when p."Unit Name" is null then o."Name" else p."Unit Name" end AS "Property Name"
        ,p."Well Name" as "Prodview Well Name"
        ,p."Property Number"
        ,w."Regulatory Effective Date"
        ,p."Regulatory Field Name"
        ,case
            when p."Rig Release Date" is null and not w."Rig Release Date" is null then CAST(w."Rig Release Date" AS date)
            when w."Rig Release Date" is null and not p."Rig Release Date" is null then CAST(p."Rig Release Date" AS date)
            else CAST(w."Rig Release Date" AS date)
        end as "Rig Release Date"    
        ,r."Route Record ID"
        ,r."Route Name"
        ,CAST(w."Spud Date" AS date) as "Spud Date"
        ,w."System Lock Date"
        ,p."Unit Record ID"
        ,p."Unit Type"
        ,P."Unit Sub Type"
        ,p."Cost Center" as "Well Code"
        ,w."Well ID"
        ,case when w."Well Name" is null then o."Name" else w."Well Name" end as "Well Name"
        ,p."Legal Well Name" as "Well Name Legal"
    from prodview p
    left join wellview w 
    on p."WellView Well ID" = w."Well ID"
    left join company c
    on p."Company Code" = c.company_code
    left join prodstatus s
    on p."Unit Record ID" = s."Unit Record ID" and s.rn = 1
    left join route r
    on p."Current Route" = r."Route Record ID"
    full outer join oda o 
    on p."Cost Center" = o."Code"
),

ranked AS (
    SELECT
        t.*,
        ROW_NUMBER() OVER (
            PARTITION BY "Unit Record ID"
            ORDER BY "Last Mod Date (UTC)" DESC
        ) AS rn
    FROM tbl t
)

SELECT *
    ,concat(floor("Asset Company Code"), ':', ' ', "Asset Company") as "Asset Company Full Name"
FROM ranked
WHERE rn = 1
--and "Asset Company" LIKE '%King%'