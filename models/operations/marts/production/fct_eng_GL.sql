{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Mart: Engineering GL (Power BI)

    Purpose: GL transactions shaped for Power BI engineering consumption.
    All column aliases are quoted to match the existing Power BI dataset contract.
    DO NOT rename quoted aliases — they are a published interface.

    Filters:
    - Posted transactions only
    - Journal date within last 3 years
    - LOS-mapped accounts only (via dim_accounts WHERE IN filter)
    - Non-null GL IDs

    Source migration (2026-02-20):
    Repointed from int_general_ledger_enhanced → fct_gl_details.
    Column mappings applied inline (boolean→Y/N, date→string formatting, etc.)

    Dependencies:
    - fct_gl_details (canonical GL fact)
    - dim_accounts (for combined_account WHERE IN filter)
#}

with acctmapping as (
    select * from {{ ref('dim_accounts') }}
    where is_los_account = true
),

gl as (
    select -- noqa: ST06
        gl_id as "GL ID",
        gl_description as "GL Description",
        total_interest_expected as "NRI Expected",
        net_revenue_interest_actual as "NRI Actual",
        afe_code as "AFE Number",
        voucher_type_id as "Voucher Type Code",
        location_name as "Location Name",
        location_type as "Location Type",
        company_code as "Company Code",
        company_name as "Company Name",
        search_key as "Company Asset",
        case
            when op_ref = 'NON-OPERATED' then 0
            else 1
        end as "Is Operated",
        main_account as "Main Account",
        sub_account as "Sub Account",
        concat(main_account, '-', sub_account) as "Combined Account",
        afe_type_code as "AFE Type Code",
        afe_type_label as "AFE Type Label",
        afe_type_full_name as "AFE Type Full Name",
        case when is_posted then 'Y' else 'N' end as "Posted",
        to_char(posted_at, 'MM-DD-YYYY') as "Posted Date",
        journal_date as "Journal Date",
        journal_date as "Journal Date Key",
        cash_date as "Cash Date",
        concat(voucher_code, '-', entry_sequence) as "Jordan-Key",
        voucher_code as "Voucher Code",
        is_include_in_accrual_report as "In Accrual Report",
        is_present_in_accrual_balance as "In Accrual Balance",
        accrual_date as "Accrual Date",
        accrual_date_key as "Accrual Date Key",
        net_amount as "Net Value",
        net_volume as "Net Volume",
        gross_amount as "Gross Value",
        gross_volume as "Gross Volume",
        reference as "Reference Number",
        payment_type_code as "Reference Type",
        well_code as "Well Code",
        well_name as "Well Name",
        case
            when accrual_date is null then 'UNCLOSED'
            else 'CLOSED'
        end as "Closed"
    from {{ ref('fct_gl_details') }}
),

tbl as (
    select
        "Accrual Date",
        "Accrual Date Key",
        "AFE Number",
        "AFE Type Code",
        "AFE Type Full Name",
        "AFE Type Label",
        "Cash Date",
        "Closed",
        "Combined Account",
        "Company Asset",
        "Company Code",
        "Company Name",
        "GL ID",
        "GL Description",
        "Gross Value",
        "Gross Volume",
        "In Accrual Balance",
        "In Accrual Report",
        "Is Operated",
        "Jordan-Key",
        "Journal Date",
        "Journal Date Key",
        "Location Name",
        "Location Type",
        "Main Account",
        "Net Value",
        "Net Volume",
        "Posted",
        "Posted Date",
        "Reference Number",
        "Reference Type",
        "Sub Account",
        "NRI Actual",
        "NRI Expected",
        "Voucher Code",
        "Voucher Type Code",
        "Well Code",
        "Well Name"
    from gl
    where
        "Posted" = 'Y'
        and "Journal Date" > last_day(dateadd(year, -3, current_date()), year)
),

filteraccounts as (
    select tbl.*
    from tbl
    where
        tbl."Combined Account" in (
            select combined_account from acctmapping
        )
)

select
    *,
    case
        when "Well Code" is null then cast(floor("Company Code") as varchar)
        when "Company Code" is null then "Well Code"
        else cast(
            concat(
                cast(floor("Company Code") as varchar),
                '-',
                cast("Well Code" as varchar)
            ) as varchar
        )
    end as "Asset-Well Key"
from filteraccounts
where "GL ID" is not null
