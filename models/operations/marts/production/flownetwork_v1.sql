{{
  config(
    materialized='table',
    alias='flownetwork_v1'
  )
}}

select * from {{ ref('int_prodview__flownetwork_v1') }}