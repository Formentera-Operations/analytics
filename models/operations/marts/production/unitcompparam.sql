{{
  config(
    materialized='incremental',
    unique_key='IDREC',
    on_schema_change='fail',
    alias='pvunitcompparam'
  )
}}

select * from {{ ref('stg_prodview__pvunitcompparam') }}

{% if is_incremental() %}
  where update_date > (select max(update_date) from {{ this }})
{% endif %}