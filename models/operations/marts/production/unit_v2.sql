{{
  config(
    full_refresh=false,
    materialized='table',
    alias='unit_v2'
  )
}}

select * from {{ ref('int_prodview__unit_v2') }}