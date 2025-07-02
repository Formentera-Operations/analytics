{{
  config(
    materialized='incremental',
    unique_key='IDREC',
    on_schema_change='fail',
    alias='pvunitcompdowntime'
  )
}}

select * from {{ ref('stg_prodview__completion_downtimes') }}

{% if is_incremental() %}
  where update_date > (select max(update_date) from {{ this }})
{% endif %}