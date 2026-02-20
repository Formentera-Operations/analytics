{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

WITH accounts as (
    Select
        *
    from {{ ref('dim_accounts') }}
    where IS_LOS_ACCOUNT = TRUE
)
,
rename as (
    select
        account_id AS "Account ID",
        account_code AS "Account Code",
        account_name AS "Account Name",
        account_full_name AS "Account Full Name",
        main_account AS "Main Account",
        sub_account AS "Sub Account",
        combined_account AS "Combined Account",
        is_active AS "Is Active",
        is_normally_debit AS "Is Normally Debit",
        is_accrual AS "Is Accrual",
        type_code AS "Type Code",
        type_name AS "Type Name",
        subtype_code AS "Subtype Code",
        subtype_name AS "Subtype Name",
        los_line_item_name AS "LOS Line Item Name",
        los_product_type AS "LOS Product Type",
        los_volume_report_header AS "LOS Volume Report Header",
        los_volume_line_number AS "LOS Volume Line Number",
        los_value_line_number AS "LOS Value Line Number",
        los_volume_report_header_line_number AS "LOS Volume Report Header Line Number",
        los_line_number AS "LOS Line Number",
        has_volume_reporting AS "Has Volume Reporting",
        has_value_reporting AS "Has Value Reporting",
        is_los_subtraction AS "Is LOS Subtraction",
        is_los_calculated AS "Is LOS Calculated",
        financial_statement_type AS "Financial Statement Type",
        is_income_statement_account AS "Is Income Statement Account",
        is_balance_sheet_account AS "Is Balance Sheet Account",
        los_key_sort AS "LOS Key Sort",
        los_report_header AS "LOS Report Header",
        los_report_header_line_number AS "LOS Report Header Line Number",
        is_los_account AS "Is LOS Account",
        los_category AS "LOS Category",
        case
            when los_volume_report_header in ('OIL BARRELS', 'GAS MCF', 'NGL GALLONS', 'NGL BARRELS') then 'Production Volumes'
            when los_category in ('Revenue', 'Other Income') then 'Total Revenue'
            when los_category in ('Production & Ad Valorem Taxes', 'Workover Expenses', 'P & A Expenses', 'Lease Operating Expenses') then 'Total Expenses'
            else los_category end as "LOS Title",
        los_section AS "LOS Section",
        interest_type AS "Interest Type",
        commodity_type AS "Commodity Type",
        expense_classification AS "Expense Classification",
        _refreshed_at AS "Refreshed At",
        los_category_line_number AS "LOS Category Line Number"
    FROM accounts
)

Select *
from rename