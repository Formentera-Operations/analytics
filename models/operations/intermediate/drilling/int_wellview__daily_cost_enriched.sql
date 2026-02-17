-- Enriches daily cost line items with report date, job context, and dimensional FKs.
-- Grain: 1 row per cost line item (same as stg_wellview__daily_costs).
-- Ephemeral — compiles as CTE into fct_daily_drilling_cost.

with daily_costs as (
    select * from {{ ref('stg_wellview__daily_costs') }}
),

job_reports as (
    select
        report_id,
        job_id,
        report_start_datetime::date as report_date,
        _fivetran_synced
    from {{ ref('stg_wellview__job_reports') }}
),

jobs as (
    select
        job_id,
        job_category,
        job_type_primary,
        _fivetran_synced
    from {{ ref('stg_wellview__jobs') }}
),

well_360 as (
    select
        wellview_id,
        eid
    from {{ ref('well_360') }}
    where wellview_id is not null
),

enriched as (
    select
        dc.daily_cost_sk,

        -- dimensional FKs (job_sk computed inline to avoid mart dependency)
        {{ dbt_utils.generate_surrogate_key(['jr.job_id']) }} as job_sk,
        w360.eid,

        -- natural keys
        dc.cost_line_id,
        dc.job_report_id,
        dc.well_id,
        jr.job_id,

        -- report context
        jr.report_date,

        -- job context
        j.job_category,
        j.job_type_primary,

        -- cost information
        dc.field_estimate_cost,
        dc.cumulative_field_estimate_cost,

        -- account coding
        dc.main_account_id,
        dc.sub_account_id,
        dc.spend_category,
        dc.expense_type,
        dc.tangible_intangible,
        dc.afe_category,
        dc.account_name,

        -- vendor
        dc.vendor_name,
        dc.vendor_code,

        -- operational
        dc.ops_category,
        dc.status,

        -- purchase/work orders
        dc.purchase_order_number,
        dc.work_order_number,
        dc.ticket_number,

        -- source freshness watermark (stable across runs for incremental merge)
        greatest(
            coalesce(dc._fivetran_synced::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(jr._fivetran_synced::timestamp_ntz, '1900-01-01'::timestamp_ntz),
            coalesce(j._fivetran_synced::timestamp_ntz, '1900-01-01'::timestamp_ntz)
        ) as source_synced_at,

        -- dbt metadata
        dc._loaded_at

    from daily_costs as dc
    left join job_reports as jr
        on dc.job_report_id = jr.report_id
    left join jobs as j
        on jr.job_id = j.job_id
    left join well_360 as w360
        on dc.well_id = w360.wellview_id
)

select * from enriched
