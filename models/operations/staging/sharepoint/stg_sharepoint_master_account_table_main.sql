{{
  config(
    materialized='view'
  )
}}

with source_data as (
    select * from {{ source('sharepoint', 'MASTER_ACCOUNT_TABLE_MAIN') }}
),

renamed as (
    Select
        _FIVETRAN_SYNCED
        ,_LINE
        ,ACCOUNT_ID as "Account_ID"
        ,ACCOUNT_NAME as "Account_Name"
        ,ACTIVE as "Active"
        ,COMBINED_ACCOUNT as "CombinedAccount"
        ,FS_CAT_1
        ,FS_CAT_2
        ,FS_CAT_3
        ,FS_CAT_4
        ,FULL_ACCOUNT as "Full_Account"
        ,FULL_NAME as "Full_Name"
        ,IS_ACCRUAL
        ,IS_GROSS
        ,IS_LOS
        ,KEY_SORT as "Key_Sort"
        ,LOS_SPEND_CAT
        ,MAIN_ACCOUNT as "Main_Account"
        ,MAIN_CAT
        ,MAIN_ORDER
        ,OPS_DEF
        ,SUB_ACCOUNT as "Sub_Account"
        ,SUB_CAT
        ,SUB_ORDER
        ,TANGIBLE_INTANGIBLE
        ,UNIQUE_ID as "Unique_ID"
        ,VAL_DEF
    from source_data
)
    
select * from renamed