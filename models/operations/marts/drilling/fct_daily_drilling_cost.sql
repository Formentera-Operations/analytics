{{
    config(
        materialized='incremental',
        unique_key='cost_line_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['well_id', 'job_id'],
        tags=['drilling', 'mart', 'fact']
    )
}}

with source as (
    select * from {{ ref('int_wellview__daily_cost_enriched') }}
    {% if is_incremental() %}
        where _loaded_at > (select max(_loaded_at) from {{ this }})
    {% endif %}
),

final as (
    select
        daily_cost_sk,

        -- dimensional FKs
        job_sk,
        eid,

        -- natural keys
        cost_line_id,
        job_report_id,
        well_id,
        job_id,

        -- temporal
        report_date,

        -- job classification
        job_category,
        job_type_primary,

        -- cost measures
        field_estimate_cost,
        cumulative_field_estimate_cost,

        -- account coding
        main_account_id,
        sub_account_id,
        spend_category,
        expense_type,
        tangible_intangible,
        afe_category,
        account_name,

        -- vendor
        vendor_name,
        vendor_code,

        -- operational
        ops_category,
        status,

        -- purchase/work orders
        purchase_order_number,
        work_order_number,
        ticket_number,

        -- dbt metadata
        _loaded_at

    from source
)

select * from final
