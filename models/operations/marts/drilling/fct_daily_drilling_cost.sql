{{
    config(
        materialized='incremental',
        unique_key='cost_line_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['eid', 'job_id'],
        tags=['drilling', 'mart', 'fact']
    )
}}

with source as (
    {% if is_incremental() %}
        {% set target_relation = adapter.get_relation(
            database=this.database,
            schema=this.schema,
            identifier=this.identifier
        ) %}
        {% set target_cols = [] %}
        {% if target_relation is not none %}
            {% set target_cols = adapter.get_columns_in_relation(target_relation) %}
        {% endif %}
        {% set target_col_names = target_cols | map(attribute='name') | list %}
    {% endif %}

    select * from {{ ref('int_wellview__daily_cost_enriched') }}
    {% if is_incremental() %}
        where
            {% if 'SOURCE_SYNCED_AT' in target_col_names or 'source_synced_at' in target_col_names %}
                source_synced_at > (
                    select coalesce(max(source_synced_at), '1900-01-01'::timestamp_ntz) from {{ this }}
                )
            {% else %}
                _loaded_at > (
                    select coalesce(max(_loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }}
                )
            {% endif %}
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

        -- source freshness watermark used for incremental merge
        source_synced_at,

        -- dbt metadata
        _loaded_at

    from source
)

select * from final
