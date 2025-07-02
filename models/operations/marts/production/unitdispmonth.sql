{{
  config(
    materialized='table',
    alias='pvunitdispmonth'
  )
}}

select * from {{ ref('stg_prodview__monthly_dispositions') }}