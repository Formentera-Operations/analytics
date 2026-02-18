{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Chart of Accounts (V2)

    Source: ODA_BATCH_ODA_ACCOUNT_V2 (~2K rows, batch)
    Grain: One row per account (id)

    Notes:
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - changed_since_printed kept as int (may not be 0/1)
    - is_accrual derived from account_name pattern in enhanced CTE
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNT_V2') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ACCOUNTV2IDENTITY::int as account_v2_identity,
        NID::int as n_id,

        -- account attributes
        trim(CODE)::varchar as account_code,
        trim(NAME)::varchar as account_name,
        trim(FULLNAME)::varchar as account_full_name,
        trim(MAINACCOUNT)::varchar as main_account,
        trim(SUBACCOUNT)::varchar as sub_account,

        -- sorting
        ACCOUNTSORT::varchar as account_sort,
        KEYSORT::varchar as key_sort,

        -- classification
        ACCOUNTTYPEID::int as account_type_id,
        ACCOUNTSUBTYPEID::int as account_subtype_id,

        -- flags
        coalesce(ACTIVE = 1, false) as is_active,
        coalesce(NORMALLYDEBIT = 1, false) as is_normally_debit,
        coalesce(SUMMARIZEINGLREPORTS = 1, false) as is_summarize_in_gl_reports,
        coalesce(SUMMARIZEINJIBINVOICE = 1, false) as is_summarize_in_jib_invoice,
        nullif(JIBSUMMARYID, '')::varchar as jib_summary_id,
        coalesce(BANKRECONPERFORMED = 1, false) as is_bank_recon_performed,
        CHANGEDSINCEPRINTED::int as changed_since_printed,
        coalesce(GENERATEFIXEDASSETCANDIDATES = 1, false) as is_generate_fixed_asset_candidates,
        coalesce(REMEASUREATCURRENT = 1, false) as is_remeasure_at_current,
        coalesce(TRANSLATEATCURRENT = 1, false) as is_translate_at_current,

        -- usage types
        AFEUSAGETYPEID::int as afe_usage_type_id,
        ALTERNATEREFERENCEUSAGETYPEID::int as alternate_reference_usage_type_id,
        ENTRYCLASSIFICATIONUSAGETYPEID::int as entry_classification_usage_type_id,
        EXPENSEDISTRIBUTIONUSAGETYPEID::int as expense_distribution_usage_type_id,
        MANUALENTRYUSAGETYPEID::int as manual_entry_usage_type_id,
        WELLUSAGETYPEID::int as well_usage_type_id,

        -- additional classification
        LOCATIONGROUPINGID::int as location_grouping_id,
        LASTCHANGECODE::int as last_change_code,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,

        -- estuary metadata
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as account_v2_sk,
        *,
        lower(account_name) like '%accru%' as is_accrual,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        account_v2_sk,

        -- identifiers
        id,
        account_v2_identity,
        n_id,

        -- account attributes
        account_code,
        account_name,
        account_full_name,
        main_account,
        sub_account,

        -- sorting
        account_sort,
        key_sort,

        -- classification
        account_type_id,
        account_subtype_id,

        -- flags
        is_active,
        is_normally_debit,
        is_accrual,
        is_summarize_in_gl_reports,
        is_summarize_in_jib_invoice,
        jib_summary_id,
        is_bank_recon_performed,
        changed_since_printed,
        is_generate_fixed_asset_candidates,
        is_remeasure_at_current,
        is_translate_at_current,

        -- usage types
        afe_usage_type_id,
        alternate_reference_usage_type_id,
        entry_classification_usage_type_id,
        expense_distribution_usage_type_id,
        manual_entry_usage_type_id,
        well_usage_type_id,

        -- additional classification
        location_grouping_id,
        last_change_code,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
