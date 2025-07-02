{{
  config(
    materialized='table',
    alias='pvunitallocmonth'
  )
}}

select * from {{ ref('stg_prodview__monthly_allocations') }}