{{ config(
    enabled=true,
    materialized='table'
) }}

select * from {{ ref('int_sp_master_account_table_main') }}