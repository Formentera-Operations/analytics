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
    select *
    from {{ ref('dim_companies') }}
),

oda as (
    select *
    from {{ ref('int_oda_wells') }}
    where not "PropertyReferenceCode" = 'DNU'
),

prodstatus as (
    select
        "Unit Record ID",
        "Status Record ID",
        "Last Mod At (UTC)",
        row_number() over (
            partition by "Unit Record ID"
            order by "Last Mod At (UTC)" desc
        ) as rn
    from {{ ref('int_prodview__production_volumes') }}
),

route as (
    select *
    from {{ ref('int_dim_route') }}
),

tbl as (
    select
        w."Abandon Date",
        p.completion_record_id as "Completion Record ID",
        cast(w."On Production Date" as date) as "First Prod Date",
        p.current_facility_id as "Current Facility",
        p.facility_name as "Facility Name",
        r.foreman as "Foreman",
        p.foreman_area as "Foreman Area",
        w."Last Approved MIT Date",
        w."Last Write To Database",
        w."Lat/Long Datum",
        w."Latitude Degrees",
        w."Longitude Degrees",
        o.SEARCHKEY as "ODA Asset",
        o.FIELD as "ODA Field",
        w."UTM Easting Meters",
        w."UTM Northing Meters",
        w."Master Lock Date",
        cast(w."Ops Effective Date" as date) as "Ops Effective Date",
        w."Permit Date",
        s."Status Record ID" as "Prod Status Record ID",
        p.producing_method as "Producing Method",
        p.property_eid as "Property EID",
        p.well_name as "PV Well Name",
        p.property_number as "Property Number",
        w."Regulatory Effective Date",
        p.regulatory_field_name as "Regulatory Field Name",
        r.id_rec as "Route Record ID",
        r.route_name as "Route Name",
        cast(w."Spud Date" as date) as "Spud Date",
        w."System Lock Date",
        p.unit_record_id as "Unit Record ID",
        w."Well ID",
        coalesce(p.api_10, o."ApiNumber") as "API 10",
        case
            when c.company_name is null and p.asset_company is null
                then
                    (case
                        when lower(o."CompanyName") = 'fp goldsmith' then 'FP Goldsmith LP'
                        when lower(o."CompanyName") = 'fp wheeler upstream' then 'FP Wheeler Upstream LLC'
                        else o."CompanyName"
                    end)
            when c.company_name is null and not p.asset_company is null
                then
                    (case
                        when lower(p.asset_company) = 'fp south texas' then 'Formentera South Texas, LP'
                        when lower(p.asset_company) = 'fp balboa la' then 'FP Balboa LA LLC'
                        when lower(p.asset_company) = 'fp balboa la midstream' then 'FP Balboa LA Midstream'
                        when lower(p.asset_company) = 'fp balboa ms' then 'FP Balboa MS LLC'
                        when lower(p.asset_company) = 'fp balboa nd' then 'FP Balboa ND LLC'
                        when lower(p.asset_company) = 'fp divide' then 'FP Divide LLC'
                        when lower(p.asset_company) = 'fp drake' then 'FP Drake LLC'
                        when lower(p.asset_company) = 'fp goldsmith' then 'FP Goldsmith LP'
                        when lower(p.asset_company) = 'fp kingfisher' then 'FP Kingfisher LLC'
                        when lower(p.asset_company) = 'fp lariat' then 'FP Lariat, LLC'
                        when lower(p.asset_company) = 'fp maverick' then 'FP Maverick LP'
                        when lower(p.asset_company) = 'fp meramec' then 'FP Meramec LLC'
                        when lower(p.asset_company) = 'fp overlook' then 'FP Overlook LLC'
                        when lower(p.asset_company) = 'fp pronghorn' then 'FP Pronghorn LLC'
                        when lower(p.asset_company) = 'fp wheeler' then 'FP Wheeler Upstream LLC'
                        when lower(p.asset_company) = 'fp wheeler midstream' then 'FP Wheeler Upstream LLC'
                        when lower(p.asset_company) = 'fp wheeler upstream' then 'FP Wheeler Upstream LLC'
                        when lower(p.asset_company) = 'snyder drillco' then 'Snyder Drill Co LP'
                        when lower(p.asset_company) = 'fp griffin' then 'FP Griffin'
                        else p.asset_company
                    end)
            when c.company_name is null and o."CompanyName" is null and p.asset_company is null then w."Asset Company"
            else c.company_name
        end as "Asset Company",
        case
            when c.company_code is null and p.asset_company is null then o."CompanyCode"
            when c.company_code is null and not p.asset_company is null
                then
                    (case
                        when lower(p.asset_company) = 'fp south texas' then 810
                        when lower(p.asset_company) = 'fp balboa la' then 707
                        when lower(p.asset_company) = 'fp balboa la midstream' then 706
                        when lower(p.asset_company) = 'fp balboa ms' then 708
                        when lower(p.asset_company) = 'fp balboa nd' then 709
                        when lower(p.asset_company) = 'fp divide' then 701
                        when lower(p.asset_company) = 'fp drake' then 813
                        when lower(p.asset_company) = 'fp goldsmith' then 807
                        when lower(p.asset_company) = 'fp lariat' then 811
                        when lower(p.asset_company) = 'fp kingfisher' then 704
                        when lower(p.asset_company) = 'fp maverick' then 703
                        when lower(p.asset_company) = 'fp meramec' then 804
                        when lower(p.asset_company) = 'fp overlook' then 800
                        when lower(p.asset_company) = 'fp pronghorn' then 809
                        when lower(p.asset_company) = 'fp wheeler' then 300
                        when lower(p.asset_company) = 'fp wheeler midstream' then 300
                        when lower(p.asset_company) = 'fp wheeler upstream' then 300
                        when lower(p.asset_company) = 'snyder drillco' then 500
                        when lower(p.asset_company) = 'fp griffin' then 818
                        else left(p.property_number, 3)
                    end)
            when c.company_code is null and o."CompanyCode" is null and p.asset_company is null then w."Company Code"
            else c.company_code end
            as "Asset Company Code",
        coalesce(p.completion_status, o."WellStatusTypeName")
            as "Completion Status",
        coalesce(p.district, o.SEARCHKEY) as "Business Unit",
        coalesce(p.unit_created_at_utc, o."Created Date")
            as "Unit Create Date (UTC)",
        case
            when p.first_sale_date is null and not w."First Sales Date" is null then cast(w."First Sales Date" as date)
            when w."First Sales Date" is null and not p.first_sale_date is null then cast(p.first_sale_date as date)
            else cast(w."First Sales Date" as date)
        end as "First Sales Date",
        coalesce(p.is_operated, o.OP_IS_OPERATED) as "PV Is Operated",
        greatest(
            coalesce(p.last_modified_at_utc, '0000-01-01T00:00:00.000Z'),
            coalesce(w."Last Mod At (UTC)", '0000-01-01T00:00:00.000Z'),
            coalesce(o."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z')
        ) as "Last Mod Date (UTC)",
        coalesce(p.unit_name, o."Name") as "Property Name",
        case
            when p.rig_release_date is null and not w."Rig Release Date" is null then cast(w."Rig Release Date" as date)
            when w."Rig Release Date" is null and not p.rig_release_date is null then cast(p.rig_release_date as date)
            else cast(w."Rig Release Date" as date)
        end as "Rig Release Date",
        coalesce(p.unit_type, o."PropertyReferenceCode") as "Unit Type",
        coalesce(p.unit_sub_type, o."CostCenterTypeName") as "Unit Sub Type",
        coalesce(p.cost_center, o."Code") as "Well Code",
        coalesce(w."Well Name", o."Name") as "Well Name",
        coalesce(p.legal_well_name, o."LegalDescription") as "Well Name Legal"
    from prodview as p
    left join wellview as w
        on p.wellview_well_id = w."Well ID"
    left join company as c
        on p.company_code = c.company_code
    left join prodstatus as s
        on p.unit_record_id = s."Unit Record ID" and s.rn = 1
    left join route as r
        on p.current_route_id = r.id_rec
    full outer join oda as o
        on p.cost_center = o."Code"
),

assetwellcode as (
    select
        "API 10",
        "Abandon Date",
        "Asset Company",
        "Asset Company Code",
        "Business Unit",
        "Completion Record ID",
        "Completion Status",
        "Current Facility",
        "Facility Name",
        "First Prod Date",
        "First Sales Date",
        "Foreman",
        "Foreman Area",
        "Last Approved MIT Date",
        "Last Mod Date (UTC)",
        "Last Write To Database",
        "Lat/Long Datum",
        "Latitude Degrees",
        "Longitude Degrees",
        "Master Lock Date",
        "ODA Asset",
        "ODA Field",
        "Ops Effective Date",
        "Permit Date",
        "Prod Status Record ID",
        "Producing Method",
        "Property EID",
        "Property Name",
        "Property Number",
        "PV Is Operated",
        "PV Well Name",
        "Regulatory Effective Date",
        "Regulatory Field Name",
        "Rig Release Date",
        "Route Name",
        "Route Record ID",
        "Spud Date",
        "System Lock Date",
        "Unit Create Date (UTC)",
        "Unit Sub Type",
        "Unit Type",
        "UTM Easting Meters",
        "UTM Northing Meters",
        "Well Code",
        "Well ID",
        "Well Name",
        "Well Name Legal",
        concat(floor("Asset Company Code"), ':', ' ', "Asset Company") as "Asset Company Full Name",
        case
            when "Well Code" is null then cast(floor("Asset Company Code") as varchar)
            when "Asset Company Code" is null then "Well Code"
            else
                cast(concat(cast(floor("Asset Company Code") as varchar), '-', cast("Well Code" as varchar)) as varchar)
        end as "Asset-Well Key",
        coalesce("Unit Record ID", (
            case
                when "Well Code" is null then cast(floor("Asset Company Code") as varchar)
                when "Asset Company Code" is null then "Well Code"
                else
                    cast(
                        concat(
                            cast(floor("Asset Company Code") as varchar), '-', cast("Well Code" as varchar)
                        ) as varchar
                    )
            end
        )) as "Unit Record ID"
    from tbl
),

ranked as (
    select
        *,
        row_number() over (
            partition by "Unit Record ID"
            order by "Last Mod Date (UTC)" desc
        ) as rn
    from assetwellcode
)

select *
from ranked
where rn = 1
