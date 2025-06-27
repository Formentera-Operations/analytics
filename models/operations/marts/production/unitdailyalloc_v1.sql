{{
  config(
    materialized='incremental',
    unique_key='IDREC',
    on_schema_change='fail',
    alias='unitdailyalloc_v1'
  )
}}

select * from {{ ref('int_prodview__unitdailyalloc_v1') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where update_date > (select max(update_date) from {{ this }})
{% endif %}