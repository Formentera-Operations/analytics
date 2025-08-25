{{
    config(
        enable=true,
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
    from {{ ref('dim_companies') }} c
),

tbl as (
    Select
        w."Abandon Date"
        ,p."API 10"
        /*,case
            when p."AssetCo" is null and not w."Asset Company" is null then w."Asset Company"
            when w."Asset Company" is null and not p."AssetCo" is null then p."AssetCo"
            else p."AssetCo"
        end as "Asset Company"*/
        ,c.company_code as "Asset company Code"
        ,c.company_name as "Asset Company"
        ,c.company_full_name as "Asset Company full Name"
        --,w."Asset Company"
        --,p."AssetCo"
        ,p."District" AS "Business Unit"
        --,w."District"
        ,w."Created At (UTC)"
        ,CAST(w."On Production Date" AS date) AS "First Prod Date"
        ,case
            when p."First Sale Date" is null and not w."First Sales Date" is null then CAST(w."First Sales Date" AS date)
            when w."First Sales Date" is null and not p."First Sale Date" is null then CAST(p."First Sale Date" AS date)
            else CAST(w."First Sales Date" AS date)
        end as "First Sales Date"           
        --,w."First Sales Date"
        --,p."First Sale Date"
        ,p."Foreman Area"
        ,p."Is Operated"
        ,w."Last Approved MIT Date"
        ,w."Last Mod At (UTC)"
        ,w."Last Write To Database"
        ,w."Master Lock Date"
        ,cast(w."Ops Effective Date" as date) as "Ops Effective Date"
        ,w."Permit Date"
        ,p."Completion Status" AS "Prod Status"
        ,p."Producing Method"
        ,p."Property EID"
        ,p."Unit Name" AS "Property Name"
        ,p."Property Number"
        ,w."Regulatory Effective Date"
        ,p."Regulatory Field Name"
        ,case
            when p."Rig Release Date" is null and not w."Rig Release Date" is null then CAST(w."Rig Release Date" AS date)
            when w."Rig Release Date" is null and not p."Rig Release Date" is null then CAST(p."Rig Release Date" AS date)
            else CAST(w."Rig Release Date" AS date)
        end as "Rig Release Date"    
        --,w."Rig Release Date"
        --,p."Rig Release Date"
        ,p."Route"
        --,p."Route Name"
        ,CAST(w."Spud Date" AS date) as "Spud Date"
        --,p."Spud Date"
        ,w."System Lock Date"
        ,p."Unit Record ID"
        ,p."Unit Type"
        ,p."Cost Center" as "Well Code"
        ,w."Well ID"
        ,w."Well Name"
    from prodview p
    left join wellview w 
    on p."WellView Well ID" = w."Well ID"
    left join company c
    on CAST(LEFT(p."Cost Center", 3) as text) = CAST(c.company_code as text)
)

select*
from tbl