{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}


WITH acctmapping as (
    select * from {{ ref('dim_accounts') }}
    where is_los_account = true
),

gl as (
    Select
        gl_id as "GL ID"
        ,gl_description as "GL Description"
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
        ,afe_type_code AS "AFE Type Code"
        ,afe_type_label AS "AFE Type Label"
        ,afe_type_full_name AS "AFE Type Full Name"
        ,posted as "Posted"
        ,posted_date as "Posted Date"
        ,journal_date as "Journal Date"
        ,journal_date_key as "Journal Date Key"
        ,cash_date as "Cash Date"
        ,CONCAT(voucher_code, '-', entry_seq) as "Jordan-Key"
        ,voucher_code as "Voucher Code"
        ,include_in_accrual_report as "In Accrual Report"
        ,present_in_accrual_balance as "In Accrual Balance"
        ,accrual_date as "Accrual Date"
        ,accrual_date_key as "Accrual Date Key"
        ,net_amount as "Net Value"
        ,net_volume as "Net Volume"
        ,gross_amount as "Gross Value"
        ,gross_volume as "Gross Volume"
        ,reference as "Reference Number"
        ,reference_type as "Reference Type"
        ,well_code as "Well Code"
        ,well_name as "Well Name"
        ,CASE
            WHEN accrual_date is null then 'UNCLOSED'
            ELSE 'CLOSED'
        END as "Closed"
    FROM {{ ref('int_general_ledger_enhanced') }}
)

, tbl as (
Select
    "Accrual Date"
    ,"Accrual Date Key"
    ,"AFE Number"
    ,"AFE Type Code"
    ,"AFE Type Full Name"
    ,"AFE Type Label"
    ,"Cash Date"
    ,"Closed"
    ,"Combined Account"
    ,"Company Asset"
    ,"Company Code"
    ,"Company Name"
    ,"GL ID"
    ,"GL Description"
    ,"Gross Value"
    ,"Gross Volume"
    ,"In Accrual Balance"
    ,"In Accrual Report"
    ,"Is Operated"
    ,"Jordan-Key"
    ,"Journal Date"
    ,"Journal Date Key"
    ,"Location Name"
    ,"Location Type"
    ,"Main Account"
    ,"Net Value"
    ,"Net Volume"
    ,"Posted"
    ,"Posted Date"
    ,"Reference Number"
    ,"Reference Type"
    ,"Sub Account"
    ,"NRI Actual"
    ,"NRI Expected"
    ,"Voucher Code"
    ,"Voucher Type Code"
    ,"Well Code"
    ,"Well Name"
from gl
WHERE "Posted" = 'Y'
AND "Journal Date" > LAST_DAY(DATEADD(year, -3,CURRENT_DATE()), year)
--AND "Main Account" in (310,311,312,313,314,315,316,317,328,701,702,703,840,850,860,870,704,900,715,901,807,903,830,806,802,318,935,704,705)
--AND NOT "Combined Account" in ('850-35', '850-36')
  --AND NOT "Company Code" IN (705, 801, 900, 200)
),

filteraccounts as (
    select
        tbl.*
    from tbl
    RIGHT JOIN acctmapping m
    on tbl."Combined Account" = m.combined_account
)

select
    *
    ,case 
        when "Well Code" is null then cast(floor("Company Code") as varchar)
        when "Company Code" is null then "Well Code"
        else cast(concat(cast(floor("Company Code") as varchar), '-' ,cast("Well Code" as varchar)) as varchar)
    end as "Asset-Well Key"
from filteraccounts
Where not "GL ID" is null