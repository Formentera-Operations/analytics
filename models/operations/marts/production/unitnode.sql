{{
  config(
    materialized='table',
    alias='pvunitnode'
  )
}}

select * from {{ ref('stg_prodview__unit_nodes') }}