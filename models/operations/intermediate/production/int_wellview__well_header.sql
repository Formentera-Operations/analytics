{{
  config(
    enable=true,
    materialized='view'
  )
}}

with header as (
    select * from {{ ref('stg_wellview__well_header') }}
),

oda_company as (
    select 
        w.well_id
        ,w.well_code
        ,c.company_code as "oda Company Code"
        ,c.company_name as "Company Name"
        ,c.company_full_name as "Company full Name"
    from {{ ref('dim_wells') }} w
    left join {{ ref('dim_companies') }} c
    on CAST(LEFT(w.well_code, 3) as text) = CAST(c.company_code as text)
),

wvintegration as (
    select 
        wi.*
        ,u.* 
    from {{ ref('stg_prodview__system_integrations') }} wi
    left join  {{ ref('stg_prodview__units') }} u
        on u."Unit Record ID"= wi."System Integration Parent Record ID" 
        and wi."Flow Net ID" = u."Flow Net ID"
    where "Product Description" = 'WellView' 
    and "System Integration Table Key" = 'pvunit'
),

source as (
    select
        h.*
        ,wi."Unit Record ID"
        ,c."oda Company Code"
        ,c."Company Name"
        ,c."Company full Name"
        -- Update tracking
        ,greatest(
            coalesce(h."Last Mod At (UTC)", '0000-01-01T00:00:00.000Z'))
         as "Updated Date" 
    from header h
        left join wvintegration wi
        on h."Well ID" =  wi."AF ID Rec"
        left join oda_company c
        on h."Well ID" = c.well_id
)

Select *
from source
order by "Well ID" Desc