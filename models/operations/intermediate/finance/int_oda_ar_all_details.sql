{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Combined AR Details — unions all AR transaction types into a single dataset

    Purpose: Standardize column order across ArInvoice, PaymentDetails,
    NetDetails, and AdjustmentDetails into a single queryable model.

    Note: Explicit column lists required because the 4 upstream models
    define columns in different orders. SELECT * with UNION ALL resolves
    by position, not name, causing type mismatches.

    Dependencies:
    - int_oda_ar_invoice
    - int_oda_ar_payments
    - int_oda_ar_netting
    - int_oda_ar_adjustments
#}

with ar_all_details as (

    select
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_id,
        invoice_type_id,
        voucher_id,
        invoice_date,
        total_invoice_amount,
        hold_billing,
        invoice_description,
        invoice_type,
        sort_order
    from {{ ref('int_oda_ar_invoice') }}

    union all

    select
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_id,
        invoice_type_id,
        voucher_id,
        invoice_date,
        total_invoice_amount,
        hold_billing,
        invoice_description,
        invoice_type,
        sort_order
    from {{ ref('int_oda_ar_payments') }}

    union all

    select
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_id,
        invoice_type_id,
        voucher_id,
        invoice_date,
        total_invoice_amount,
        hold_billing,
        invoice_description,
        invoice_type,
        sort_order
    from {{ ref('int_oda_ar_netting') }}

    union all

    select
        company_code,
        company_name,
        owner_id,
        owner_code,
        owner_name,
        well_id,
        well_code,
        well_name,
        invoice_number,
        invoice_id,
        invoice_type_id,
        voucher_id,
        invoice_date,
        total_invoice_amount,
        hold_billing,
        invoice_description,
        invoice_type,
        sort_order
    from {{ ref('int_oda_ar_adjustments') }}

)

select * from ar_all_details
