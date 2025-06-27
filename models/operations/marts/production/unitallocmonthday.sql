{{
  config(
    materialized='incremental',
    unique_key='IDREC',
    on_schema_change='fail',
    alias='pvunitallocmonthday'
  )
}}

select * from {{ ref('stg_prodview__pvunitallocmonthday') }}

{% if is_incremental() %}
  -- this filter will only be applied on an incremental run
  where update_date > (select max(update_date) from {{ this }})
{% endif %}