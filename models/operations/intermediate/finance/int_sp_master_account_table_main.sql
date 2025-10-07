{{ config(
    enable= true,
    materialized='view'
) }}

select * from {{ ref('stg_sharepoint_master_account_table_main') }}