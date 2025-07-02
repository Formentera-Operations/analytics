{{
  config(
    materialized='table',
    alias='pvflownetheader'
  )
}}

select * from {{ ref('stg_prodview__networks') }}