{{ config(
    enable= true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH gl as (
    Select
        gl_id as "GL ID"
        ,company_code as "Company Code"
        ,company_name as "Company Name"
        ,main_account as "Main Account"
        ,sub_account as "Sub Account"
        ,CONCAT(main_account,'-', sub_account) as "Combined Account"
        ,CONCAT(main_account, '-', sub_account, '-Vol') as "Account Key"
        ,Posted
        ,journal_date as "Journal Date"
        ,CONCAT(voucher_code, '-', entry_seq) as "Jordan-Key"
        ,include_in_accrual_report as "In Accrual Report"
        ,present_in_accrual_balance as "In Accrual Balance"
        ,accrual_date as "Accrual Date"
        ,net_amount as "Net Value"
        ,well_code as "Well Code"
        ,well_name as "Well Name"
        ,CASE
            WHEN accrual_date is null then 'UNCLOSED'
            ELSE 'CLOSED'
        END as "Closed"
    FROM {{ ref('int_general_ledger_enhanced') }}
)

Select *
from gl
WHERE Posted = 'Y'
AND "Journal Date" > '2021-12-31'
AND "Main Account" in (310,311,312,313,314,315,316,317,328,701,702,703,840,850,860,870,704,900,715,901,807,903,830,806,802,318,935,704,705)