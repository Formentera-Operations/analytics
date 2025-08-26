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
        ,case
            when c.company_name is null then 
                (case
                    when lower(p."AssetCo") = 'fp south texas' then 'Formentera South Texas, LP'
                    when lower(p."AssetCo") = 'fp balboa la' then 'FP Balboa LA LLC'
                    when lower(p."AssetCo") = 'fp balboa ms' then 'FP Balboa MS LLC'
                    when lower(p."AssetCo") = 'fp balboa nd' then 'FP Balboa ND LLC'
                    when lower(p."AssetCo") = 'fp divide' then 'FP Divide LLC'
                    when lower(p."AssetCo") = 'fp drake' then 'FP Drake LLC'
                    when lower(p."AssetCo") = 'fp goldsmith' then 'FP Goldsmith LP'
                    when lower(p."AssetCo") = 'fp lariat' then 'FP Lariat, LLC'
                    when lower(p."AssetCo") = 'fp maverick' then 'FP Maverick LP'
                    when lower(p."AssetCo") = 'fp meramec' then 'FP Meramec LLC'
                    when lower(p."AssetCo") = 'fp overlook' then 'FP Overlook LLC'
                    when lower(p."AssetCo") = 'fp pronghorn' then 'FP Pronghorn LLC'
                    when lower(p."AssetCo") = 'fp wheeler' then 'FP Wheeler Upstream LLC'
                    when lower(p."AssetCo") = 'fp wheeler midstream' then 'FP Wheeler Upstream LLC'
                    when lower(p."AssetCo") = 'fp wheeler upstream' then 'FP Wheeler Upstream LLC'
                    when lower(p."AssetCo") = 'snyder drillco' then 'Snyder Drill Co LP'
                    else c.company_name end)
            when c.company_name is null and p."AssetCo" is null and not w."Asset Company" is null then w."Asset Company"
            when c.company_name is null and w."Asset Company" is null and not p."AssetCo" is null then CONCAT(SPLIT_PART(p."AssetCo", ' ', 1), ' ', INITCAP(SPLIT_PART(p."AssetCo", ' ', 2)), ' ', SPLIT_PART(p."AssetCo", ' ', 3))
            else c.company_name
        end as "Asset Company"
        ,c.company_code as "Asset company Code"
        --,c.company_name as "Asset Company"
        ,c.company_full_name as "Asset Company full Name"
        --,w."Asset Company" as "WV Asset Company"
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
        ,case
            when p."Operator" like 'Fromentera%' then 1
            when w."Operator Name" like 'Fromentera%' then 1
            when p."Is Operated" is null then 0
            else p."Is Operated"
        end as "Is Operated"
        ,p."Is Operated" as "PV Is Operated"
       /* ,p."Operated Descriptor" as "PV Operated Descriptor"
        ,w."Operated Descriptor" as "WV Operated Descriptor"
        ,p."Operator" as "PV Operator"
        ,w."Operator Name" as "WV Operator"*/
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

select 
    *
    /*"Is Operated"
    ,"PV Is Operated"
    ,"PV Operated Descriptor"
    ,"WV Operated Descriptor"
    ,"PV Operator"
    ,"WV Operator"
    ,COUNT("Is Operated")*/
from tbl
--group by 1,2,3,4,5,6