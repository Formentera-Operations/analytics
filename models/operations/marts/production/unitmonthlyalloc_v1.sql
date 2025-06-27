{{
  config(
    materialized='table',
    alias='unitmonthlyalloc_v1'
  )
}}

select * from {{ ref('int_prodview__unitmonthlyalloc_v1') }}