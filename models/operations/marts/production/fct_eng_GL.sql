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
        ,CASE
            WHEN main_account IN (701,702,703) AND sub_account IN (1, 2, 3, 4, 5) THEN
                CONCAT(main_account, '-', sub_account, '-Vol') 
            ELSE CONCAT(main_account,'-', sub_account)
        END as "Account Key"
        ,Posted
        ,journal_date as "Journal Date"
        ,CONCAT(voucher_code, '-', entry_seq) as "Jordan-Key"
        ,include_in_accrual_report as "In Accrual Report"
        ,present_in_accrual_balance as "In Accrual Balance"
        ,accrual_date as "Accrual Date"
        /*,CASE
            WHEN main_account IN (701,702,703) AND sub_account IN (1, 2, 3, 4, 5) THEN net_volume
            else net_amount
        END as "Net Value"*/
        ,net_amount as "Net Value"
        ,net_volume
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
AND NOT "Combined Account" in ('850-35', '850-36')
  AND NOT "Company Code" IN (705, 801, 900)
