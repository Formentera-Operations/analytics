{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

WITH gl as (
    Select
        gl_id as "GL ID"
        ,total_interest_expected AS "NRI Expected"
        ,net_revenue_interest_actual AS "NRI Actual"
        ,afe_code as "AFE Number"
        ,voucher_type_id as "Voucher Type Code"
        ,location_name as "Location Name"
        ,location_type as "Location Type"
        ,company_code as "Company Code"
        ,company_name as "Company Name"
        ,search_key as "Company Asset"
        ,CASE
			WHEN op_ref = 'NON-OPERATED' THEN 0
			ELSE 1
			END AS "Is Operated"
        ,main_account as "Main Account"
        ,sub_account as "Sub Account"
        ,CONCAT(main_account,'-', sub_account) as "Combined Account"
        /*,CASE
            WHEN main_account IN (701,702,703) AND sub_account IN (1, 2, 3, 4, 5) THEN
                CONCAT(main_account, '-', sub_account, '-Vol') 
            ELSE CONCAT(main_account,'-', sub_account)
        END as "Account Key"*/
        ,afe_type_code AS "AFE Type Code"
        ,afe_type_label AS "AFE Type Label"
        ,afe_type_full_name AS "AFE Type Full Name"
        ,Posted
        ,journal_date as "Journal Date"
        ,CONCAT(voucher_code, '-', entry_seq) as "Jordan-Key"
        ,include_in_accrual_report as "In Accrual Report"
        ,present_in_accrual_balance as "In Accrual Balance"
        ,accrual_date as "Accrual Date"
        ,net_amount as "Net Value"
        ,net_volume as "Net Volume"
        ,gross_amount as "Gross Value"
        ,gross_volume as "Gross Volume"
        ,well_code as "Well Code"
        ,well_name as "Well Name"
        ,CASE
            WHEN accrual_date is null then 'UNCLOSED'
            ELSE 'CLOSED'
        END as "Closed"
    FROM {{ ref('int_general_ledger_enhanced') }}
)

--, tbl as (
Select
--    "Account Key"
    "Accrual Date"
    ,"AFE Number"
    ,"AFE Type Code"
    ,"AFE Type Full Name"
    ,"AFE Type Label"
    ,"Closed"
    ,"Combined Account"
    ,"Company Asset"
    ,"Company Code"
    ,"Company Name"
    ,"GL ID"
    ,"Gross Value"
    ,"Gross Volume"
    ,"In Accrual Balance"
    ,"In Accrual Report"
    ,"Is Operated"
    ,"Jordan-Key"
    ,"Journal Date"
    ,"Location Name"
    ,"Location Type"
    ,"Main Account"
    ,"Net Value"
    ,"Net Volume"
    ,"POSTED"
    ,"Sub Account"
    ,"NRI Actual"
    ,"NRI Expected"
    ,"Voucher Type Code"
    ,"Well Code"
    ,"Well Name"
from gl
WHERE Posted = 'Y'
AND "Journal Date" > '2021-12-31'
AND "Main Account" in (310,311,312,313,314,315,316,317,328,701,702,703,840,850,860,870,704,900,715,901,807,903,830,806,802,318,935,704,705)
AND NOT "Combined Account" in ('850-35', '850-36')
  AND NOT "Company Code" IN (705, 801, 900)
  --AND "Is Operated" = 1
--)

--select distinct "Company Asset" from tbl order by "Company Asset"