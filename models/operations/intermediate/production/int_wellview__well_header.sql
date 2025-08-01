{{
  config(
    materialized='view'
  )
}}

with header as (
    select * from {{ ref('stg_wellview__well_header') }}
)

/*wvintegration as (
    select * 
    from {{ ref('stg_prodview__system_integrations') }}
    where "Product Description" = 'WellView' 
    and "System Integration Table Key" = 'pvunit'
)*/

select*


    -- Update tracking
    /*greatest(
        coalesce(h."Last Mod Date (UTC)", '0000-01-01T00:00:00.000Z')
    ) as UPDATE_DATE */

from header h

/*left join wvintegration wi 
    on u."Unit Record ID"= wi."System Integration Parent Record ID" 
    and wi."Flow Net ID" = u."Flow Net ID"*/