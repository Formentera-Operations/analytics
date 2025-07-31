{{
  config( 
    materialized='incremental',
    unique_key= "Allocation Record ID",
    on_schema_change='fail'
  )
}}

select * from {{ ref('int_prodview__production_volumes') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where "Last Mod At" > (select max("Last Mod At") from {{ this }})
{% endif %}