{{
    config(
        materialized='incremental',
        unique_key='gl_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'los_category', 'los_section'],
        tags=['marts', 'finance', 'los']
    )
}}

{#
    Mart: Lease Operating Statement Transactions

    Purpose: Transaction-level fact table for LOS reporting
    Grain: One row per posted GL transaction on LOS-mapped accounts (gl_id)

    Key filters applied:
    - is_posted = true (only posted transactions)
    - is_los_account = true (only accounts in LOS mapping)

    Key enrichments from fct_gl_details:
    - LOS hierarchy (los_category → los_section → los_line_item_name)
    - Product type (OIL, GAS, NGL, OTHER)
    - Volume/value reporting flags
    - Report line numbers for Power BI sorting

    LOS sign convention:
    - Revenue amounts are NEGATIVE (cash received)
    - Costs (LOE, capex, etc.) are POSITIVE
    - Net income = SUM(all categories); positive = profitable

    Dependencies:
    - fct_gl_details (canonical GL fact with pre-joined account classification)
#}

with gl_posted as (
    select *
    from {{ ref('fct_gl_details') }}
    where
        is_posted = true
        and is_los_account = true
        {% if is_incremental() %}
            and _flow_published_at > (
                select coalesce(max(_flow_published_at), '1900-01-01'::timestamp_tz)
                from {{ this }}
            )
        {% endif %}
),

los_transactions as (
    select -- noqa: ST06
        -- =================================================================
        -- Keys and Metadata
        -- =================================================================
        gl_id,
        _loaded_at,
        _flow_published_at,
        _last_refresh_at,
        created_at,
        updated_at,

        -- =================================================================
        -- Company
        -- =================================================================
        company_code,
        company_name,

        -- =================================================================
        -- Account (from GL)
        -- =================================================================
        account_id,
        main_account,
        sub_account,
        account_name,

        -- =================================================================
        -- LOS Classification (from dim_accounts via fct_gl_details)
        -- =================================================================
        los_category,
        los_section,
        los_key_sort,
        los_line_item_name,
        los_product_type,
        los_volume_line_number,
        los_value_line_number,
        has_volume_reporting,
        has_value_reporting,
        is_los_subtraction,
        interest_type,
        commodity_type,
        expense_classification,

        -- =================================================================
        -- Location (Polymorphic)
        -- =================================================================
        location_type,
        location_code,
        location_name,

        -- =================================================================
        -- Entity (Polymorphic - separate IDs for Power BI joins)
        -- =================================================================
        entity_type,
        owner_entity_id,
        vendor_entity_id,
        purchaser_entity_id,
        entity_code,
        entity_name,

        -- =================================================================
        -- Well
        -- =================================================================
        well_id,
        well_code,
        well_name,
        op_ref,
        search_key,

        -- =================================================================
        -- AFE
        -- =================================================================
        afe_id,
        afe_code,
        afe_type_code,
        afe_type_label,

        -- =================================================================
        -- Dates
        -- =================================================================
        journal_date,
        journal_month_start,
        journal_year,
        accrual_date,
        accrual_month_start,
        accrual_year,
        cash_date,
        cash_month_start,
        cash_year,

        -- =================================================================
        -- Posting Info
        -- =================================================================
        voucher_id,
        voucher_code,
        posted_at,
        posted_at_cst,

        -- =================================================================
        -- Source & Reference
        -- =================================================================
        source_module_code,
        source_module_name,
        payment_type_code,
        reference,
        gl_description,

        -- =================================================================
        -- Financial Values
        -- =================================================================
        gross_amount,
        net_amount,
        gross_volume,
        net_volume,

        -- Signed values based on LOS subtraction flag
        case
            when is_los_subtraction then gross_amount * -1
            else gross_amount
        end as los_gross_amount,

        case
            when is_los_subtraction then net_amount * -1
            else net_amount
        end as los_net_amount,

        case
            when is_los_subtraction then gross_volume * -1
            else gross_volume
        end as los_gross_volume,

        case
            when is_los_subtraction then net_volume * -1
            else net_volume
        end as los_net_volume,

        -- =================================================================
        -- Revenue/Expense Deck
        -- =================================================================
        revenue_deck_revision,
        revenue_deck_effective_date,
        expense_deck_set_code,
        expense_deck_revision,
        expense_deck_effective_date,

        -- =================================================================
        -- Entry Metadata
        -- =================================================================
        is_generated_entry,
        is_allocation_parent,
        is_allocation_generated,
        entry_group,
        entry_sequence

    from gl_posted
)

select * from los_transactions
