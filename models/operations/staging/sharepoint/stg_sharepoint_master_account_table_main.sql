{{
  config(
    materialized='view'
  )
}}

    select * from {{ source('sharepoint', 'MASTER_ACCOUNT_TABLE_MAIN') }}
    