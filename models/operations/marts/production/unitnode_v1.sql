{{
  config(
    materialized='table',
    alias='unitnode_v1'
  )
}}

select * from {{ ref('int_prodview__unitnode_v1') }}