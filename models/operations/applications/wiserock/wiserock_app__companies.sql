{{ config(
    tags=['wiserock', 'companies']
) }}

with wellview as (
    select * 
    from {{ ref('stg_wiserock__wv_well_headers') }}
)

,

wvintegration as (
    select * 
    from {{ ref('stg_wiserock__pv_system_integration') }}
    where afproduct = 'WellView' 
    and tblkeyparent = 'pvunit'
)

,

prodview as (
    select
        u.*
        ,wi.afidrec
    from {{ ref('stg_wiserock__pv_units') }} u
    left join wvintegration wi 
    on u.idrec= wi.idrecparent
    and wi.idflownet = u.idflownet
)

,

companies as (
    select 
        id,
        code,
        name,
        full_name,
    from {{ ref('stg_oda__company_v2') }}
)

,

tbl as (
    select
        p.idrec as "Unit ID"
        ,p.afidrec as "Wellview Well ID"
        ,case 
            when c.name is null then
                (case
                    when lower(p.area) = 'fp south texas' then 'Formentera South Texas, LP'
                    when lower(p.area) = 'fp balboa la' then 'FP Balboa LA LLC'
                    when lower(p.area) = 'fp balboa ms' then 'FP Balboa MS LLC'
                    when lower(p.area) = 'fp balboa nd' then 'FP Balboa ND LLC'
                    when lower(p.area) = 'fp divide' then 'FP Divide LLC'
                    when lower(p.area) = 'fp drake' then 'FP Drake LLC'
                    when lower(p.area) = 'fp goldsmith' then 'FP Goldsmith LP'
                    when lower(p.area) = 'fp lariat' then 'FP Lariat, LLC'
                    when lower(p.area) = 'fp maverick' then 'FP Maverick LP'
                    when lower(p.area) = 'fp meramec' then 'FP Meramec LLC'
                    when lower(p.area) = 'fp overlook' then 'FP Overlook LLC'
                    when lower(p.area) = 'fp pronghorn' then 'FP Pronghorn LLC'
                    when lower(p.area) = 'fp wheeler' then 'FP Wheeler Upstream LLC'
                    when lower(p.area) = 'fp wheeler midstream' then 'FP Wheeler Upstream LLC'
                    when lower(p.area) = 'fp wheeler upstream' then 'FP Wheeler Upstream LLC'
                    when lower(p.area) = 'snyder drillco' then 'Snyder Drill Co LP'
                    else c.name end)
            when c.name is null and p.area is null and not w.area is null then w.area
            else c.name
        end as "Asset Company"
        ,case
            when c.code is null then
                (case
                    when lower(p.area) = 'fp south texas' then 810
                    when lower(p.area) = 'fp balboa la' then 707
                    when lower(p.area) = 'fp balboa ms' then 708
                    when lower(p.area) = 'fp balboa nd' then 709
                    when lower(p.area) = 'fp divide' then 701
                    when lower(p.area) = 'fp drake' then 813
                    when lower(p.area) = 'fp goldsmith' then 807
                    when lower(p.area) = 'fp lariat' then 811
                    when lower(p.area) = 'fp maverick' then 703
                    when lower(p.area) = 'fp meramec' then 804
                    when lower(p.area) = 'fp overlook' then 800
                    when lower(p.area) = 'fp pronghorn' then 809
                    when lower(p.area) = 'fp wheeler' then 300
                    when lower(p.area) = 'fp wheeler midstream' then 300
                    when lower(p.area) = 'fp wheeler upstream' then 300
                    when lower(p.area) = 'snyder drillco' then 500
                    else c.code end)
                when c.code is null and p.area is null and not w.divisioncode is null then w.divisioncode
            else c.code end
        as "Asset Company Code"
    from prodview p
    left join wellview w 
    on p.afidrec = w.idwell
    left join companies c
    on p.divisioncode = c.code
)

select 
    "Unit ID"
    ,"Wellview Well ID"
    ,"Asset Company"
    ,floor("Asset Company Code") as "Asset Company Code"
    ,concat(floor("Asset Company Code"), ':', ' ', "Asset Company") as "Asset Company Full Name"
from tbl

