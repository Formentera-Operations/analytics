{{
  config(
    enable=true,
    materialized='view'
  )
}}

with header as (
    select * from {{ ref('stg_wellview__well_header') }}
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
        -- Update tracking
        ,greatest(
            coalesce(h."Last Mod At (UTC)", '0000-01-01T00:00:00.000Z'))
         as "Updated Date" 
    from header h
        left join wvintegration wi
        on h."Well ID" =  wi."AF ID Rec"
)

Select *
from source
order by "Well ID" Desc