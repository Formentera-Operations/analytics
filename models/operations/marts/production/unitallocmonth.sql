{{
  config(
    materialized='table',
    alias='pvunitallocmonth'
  )
}}

select * from {{ ref('stg_prodview__pvunitallocmonth') }}