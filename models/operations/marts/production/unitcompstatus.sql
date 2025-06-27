{{
  config(
    materialized='incremental',
    unique_key='IDREC',
    on_schema_change='fail',
    alias='pvunitcompstatus'
  )
}}

select * from {{ ref('stg_prodview__pvunitcompstatus') }}

{% if is_incremental() %}
  where update_date > (select max(update_date) from {{ this }})
{% endif %}