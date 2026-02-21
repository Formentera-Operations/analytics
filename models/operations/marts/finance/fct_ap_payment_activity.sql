{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'ap']
    )
}}

{#
    Mart: Accounts Payable Payment Activity

    Purpose: Invoice-level AP fact with aggregated check payment allocations.
    One row per AP invoice enriched with payment totals, check counts, and
    derived status fields for payment tracking and aging analysis.

    Grain: One row per AP invoice (apinvoice.id)
    Row count: ~336K invoices, with 97.1% having check allocations

    Build pattern:
    1. invoice_base: All invoice-level columns from stg_oda__apinvoice
    2. check_allocations: Aggregate stg_oda__ap_check_detail by invoice_id
    3. check_dates: First/last check dates via stg_oda__apcheck join
    4. final: Combine with dim lookups and derived fields

    Investigation findings (2026-02-20):
    - 97.6% of checks have detail allocations (50K / 51K)
    - 97.1% of invoices have check allocations (327K / 336K)
    - 330K total AP check detail rows

    Dependencies:
    - stg_oda__apinvoice (primary)
    - stg_oda__ap_check_detail (check-to-invoice allocations)
    - stg_oda__apcheck (check dates)
    - dim_vendors, dim_companies
#}

with

-- =============================================================================
-- Source: AP invoices
-- =============================================================================
invoice_base as (
    select
        id as invoice_id,
        ap_invoice_identity,
        company_id,
        vendor_id,
        voucher_id,
        code as invoice_code,
        description,
        invoice_date,
        due_date,
        accrual_date,
        acceptance_date,
        received_date,
        invoice_amount,
        amount_due,
        paid_amount,
        discount_allowed,
        discount_percentage,
        discount_taken_amount,
        to_be_paid,
        payment_status_code,
        payment_type_code,
        is_approved_for_posting,
        is_posted,
        is_ready_to_pay,
        is_include_in_accrual_report,
        is_from_ap_history_import,
        is_payment_detail_incomplete,
        created_at,
        updated_at,
        _loaded_at,
        _flow_published_at
    from {{ ref('stg_oda__apinvoice') }}
),

-- =============================================================================
-- Aggregate check allocations per invoice
-- =============================================================================
check_allocations as (
    select
        invoice_id,
        sum(payment_amount) as total_paid_amount,
        sum(discount_amount) as total_discount_amount,
        sum(federal_withholding) as total_federal_withholding,
        sum(state_withholding) as total_state_withholding,
        count(distinct check_id) as check_count,
        booland_agg(not is_voided) = false as has_voided_allocations
    from {{ ref('stg_oda__ap_check_detail') }}
    group by invoice_id
),

-- =============================================================================
-- Check date range per invoice (via check_detail → check join)
-- =============================================================================
check_dates as (
    select
        cd.invoice_id,
        min(chk.issued_date) as first_check_date,
        max(chk.issued_date) as last_check_date
    from {{ ref('stg_oda__ap_check_detail') }} cd
    inner join {{ ref('stg_oda__apcheck') }} chk
        on cd.check_id = chk.id
    where not cd.is_voided
    group by cd.invoice_id
),

-- =============================================================================
-- Dimension lookups
-- =============================================================================
vendors as (
    select
        vendor_id,
        vendor_code,
        vendor_name
    from {{ ref('dim_vendors') }}
),

companies as (
    select
        company_id,
        company_code,
        company_name
    from {{ ref('dim_companies') }}
),

-- =============================================================================
-- Enriched output
-- =============================================================================
final as (
    select
        -- =================================================================
        -- Surrogate Key
        -- =================================================================
        {{ dbt_utils.generate_surrogate_key(['i.invoice_id']) }}
            as ap_payment_activity_sk,

        -- =================================================================
        -- Invoice Identity
        -- =================================================================
        i.invoice_id,
        i.ap_invoice_identity,
        i.invoice_code,
        i.description,
        i.voucher_id,

        -- =================================================================
        -- Company
        -- =================================================================
        i.company_id,
        co.company_code,
        co.company_name,

        -- =================================================================
        -- Vendor
        -- =================================================================
        i.vendor_id,
        v.vendor_code,
        v.vendor_name,

        -- =================================================================
        -- Dates
        -- =================================================================
        i.invoice_date,
        i.due_date,
        i.accrual_date,
        i.acceptance_date,
        i.received_date,
        cd.first_check_date,
        cd.last_check_date,

        -- =================================================================
        -- Invoice Financials (from source)
        -- =================================================================
        i.invoice_amount,
        i.amount_due,
        i.paid_amount,
        i.discount_allowed,
        i.discount_percentage,
        i.discount_taken_amount,
        i.to_be_paid,

        -- =================================================================
        -- Check Allocation Aggregates
        -- =================================================================
        coalesce(ca.total_paid_amount, 0) as total_check_paid_amount,
        coalesce(ca.total_discount_amount, 0) as total_check_discount_amount,
        coalesce(ca.total_federal_withholding, 0) as total_federal_withholding,
        coalesce(ca.total_state_withholding, 0) as total_state_withholding,
        coalesce(ca.check_count, 0) as check_count,

        -- =================================================================
        -- Derived Financial
        -- =================================================================
        i.invoice_amount
        - coalesce(ca.total_paid_amount, 0)
        - coalesce(ca.total_discount_amount, 0)
            as amount_remaining,

        -- =================================================================
        -- Derived Dates
        -- =================================================================
        case
            when cd.first_check_date is not null
                then datediff('day', i.invoice_date, cd.first_check_date)
        end as days_to_first_payment,

        -- =================================================================
        -- Status Flags
        -- =================================================================
        i.payment_status_code,
        i.payment_type_code,
        i.is_approved_for_posting,
        i.is_posted,
        i.is_ready_to_pay,
        i.is_include_in_accrual_report,
        i.is_from_ap_history_import,
        i.is_payment_detail_incomplete,

        -- derived flags
        i.invoice_amount
        - coalesce(ca.total_paid_amount, 0)
        - coalesce(ca.total_discount_amount, 0) <= 0
            as is_fully_paid,

        i.due_date < current_date()
        and i.invoice_amount
        - coalesce(ca.total_paid_amount, 0)
        - coalesce(ca.total_discount_amount, 0) > 0
            as is_overdue,

        coalesce(ca.has_voided_allocations, false) as has_voided_allocations,

        -- =================================================================
        -- Audit
        -- =================================================================
        i.created_at,
        i.updated_at,
        i._loaded_at,
        i._flow_published_at

    from invoice_base i
    left join check_allocations ca on i.invoice_id = ca.invoice_id
    left join check_dates cd on i.invoice_id = cd.invoice_id
    left join vendors v on i.vendor_id = v.vendor_id
    left join companies co on i.company_id = co.company_id
)

select * from final
