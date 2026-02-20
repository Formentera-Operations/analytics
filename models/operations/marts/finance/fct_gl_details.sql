{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'main_account', 'sub_account'],
        tags=['marts', 'finance', 'gl'],
        snowflake_warehouse=set_warehouse_size('M') if target.name in ['prod', 'ci'] else target.warehouse,
    )
}}

{#
    Mart: General Ledger Details

    Purpose: Canonical GL fact table enriched with account classifications from dim_accounts.
    Single source of truth for all GL consumers (fct_los, fct_eng_GL, future models).
    Grain: One row per GL entry (gl_id)

    Key enrichments over int_gl_enhanced:
    - LOS account classification (category, section, line items)
    - Financial statement type and expense classification
    - Interest type and commodity type
    - Volume/value reporting flags

    Incremental strategy:
    - Uses _flow_published_at watermark (Estuary CDC batch timestamp)
    - Merge on gl_id; late-arriving dimension updates require periodic full refresh

    Dependencies:
    - int_gl_enhanced (source)
    - dim_accounts (account classification)
#}

with gl_enhanced as (
    select * from {{ ref('int_gl_enhanced') }}
    {% if is_incremental() %}
        where _flow_published_at > (select max(_flow_published_at) from {{ this }})
    {% endif %}
),

accounts as (
    select * from {{ ref('dim_accounts') }}
),

final as (
    select
        -- =================================================================
        -- Keys and Metadata
        -- =================================================================
        gl.gl_id,
        gl._loaded_at,
        gl._flow_published_at,
        gl.created_at,
        gl.updated_at,

        -- =================================================================
        -- Company
        -- =================================================================
        gl.company_code,
        gl.company_name,

        -- =================================================================
        -- Account (from GL)
        -- =================================================================
        gl.account_id,
        gl.main_account,
        gl.sub_account,
        gl.account_name,

        -- =================================================================
        -- Account Classification (from dim_accounts)
        -- =================================================================
        acct.is_los_account,
        acct.los_category,
        acct.los_section,
        acct.los_key_sort,
        acct.los_line_item_name,
        acct.los_product_type,
        acct.los_volume_line_number,
        acct.los_value_line_number,
        acct.has_volume_reporting,
        acct.has_value_reporting,
        acct.is_los_subtraction,
        acct.interest_type,
        acct.commodity_type,
        acct.expense_classification,
        acct.financial_statement_type,
        acct.is_income_statement_account,
        acct.is_balance_sheet_account,
        acct.los_category_line_number,
        acct.los_report_header,
        acct.los_report_header_line_number,

        -- =================================================================
        -- AFE Classification
        -- =================================================================
        gl.afe_id,
        gl.afe_code,
        gl.afe_type_id,
        gl.afe_type_code,
        gl.afe_type_label,
        gl.afe_type_full_name,

        -- =================================================================
        -- Location (Polymorphic)
        -- =================================================================
        gl.location_type,
        gl.location_code,
        gl.location_name,

        -- =================================================================
        -- Entity (Polymorphic)
        -- =================================================================
        gl.entity_type,
        gl.owner_entity_id,
        gl.vendor_entity_id,
        gl.purchaser_entity_id,
        gl.entity_code,
        gl.entity_name,

        -- =================================================================
        -- Well
        -- =================================================================
        gl.well_id,
        gl.well_code,
        gl.well_name,
        gl.op_ref,
        gl.search_key,

        -- =================================================================
        -- Posting Status
        -- =================================================================
        gl.is_posted,
        gl.voucher_id,
        gl.voucher_code,
        gl.voucher_type_id,
        gl.posted_at,
        gl.posted_at_cst,

        -- =================================================================
        -- Dates
        -- =================================================================
        gl.journal_date,
        gl.journal_month_start,
        gl.journal_year,
        gl.accrual_date,
        gl.accrual_date_key,
        gl.accrual_month_start,
        gl.accrual_year,
        gl.cash_date,
        gl.cash_month_start,
        gl.cash_year,

        -- =================================================================
        -- Source & Reference
        -- =================================================================
        gl.source_module_code,
        gl.source_module_name,
        gl.payment_type_code,
        gl.reference,
        gl.gl_description,

        -- =================================================================
        -- Financial Values
        -- =================================================================
        gl.gross_amount,
        gl.net_amount,
        gl.gross_volume,
        gl.net_volume,
        gl.currency_id,
        gl.is_currency_defaulted,

        -- =================================================================
        -- Revenue Deck
        -- =================================================================
        gl.revenue_deck_revision,
        gl.revenue_deck_effective_date,
        gl.total_interest_expected,
        gl.net_revenue_interest_actual,

        -- =================================================================
        -- Expense Deck
        -- =================================================================
        gl.expense_deck_set_code,
        gl.expense_deck_revision,
        gl.expense_deck_effective_date,
        gl.expense_deck_interest_total,

        -- =================================================================
        -- Report Inclusion Flags
        -- =================================================================
        gl.is_include_in_journal_report,
        gl.is_present_in_journal_balance,
        gl.is_include_in_cash_report,
        gl.is_present_in_cash_balance,
        gl.is_include_in_accrual_report,
        gl.is_present_in_accrual_balance,

        -- =================================================================
        -- Reconciliation
        -- =================================================================
        gl.reconciliation_type_code,
        gl.is_reconciled,
        gl.is_reconciled_trial,

        -- =================================================================
        -- Entry Metadata
        -- =================================================================
        gl.is_generated_entry,
        gl.is_allocation_parent,
        gl.is_allocation_generated,
        gl.allocation_parent_id,
        gl.entry_group,
        gl.entry_sequence,
        gl._last_refresh_at

    from gl_enhanced as gl
    left join accounts as acct
        on gl.account_id = acct.account_id
)

select * from final
