{{
  config(
    materialized='table',
    alias='unit'
  )
}}

select * from {{ ref('stg_prodview__pvunit') }}