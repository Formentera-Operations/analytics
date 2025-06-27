{{
  config(
    materialized='table',
    alias='unitallocmonth'
  )
}}

select * from {{ ref('stg_prodview__pvunit') }}