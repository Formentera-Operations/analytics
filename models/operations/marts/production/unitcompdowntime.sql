{{
  config(
    materialized='incremental',
    unique_key='id_rec',
    on_schema_change='fail',
    alias='unit_comp_downtime'
  )
}}

select * from {{ ref('stg_prodview__pvunitcompdowntm') }}

{% if is_incremental() %}
  where update_date > (select max(update_date) from {{ this }})
{% endif %}