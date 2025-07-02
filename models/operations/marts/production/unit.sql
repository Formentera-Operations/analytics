{{
  config(
    materialized='table',
    alias='pvunit'
  )
}}

select * from {{ ref('stg_prodview__units') }}