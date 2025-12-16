{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'accounting']
    )
}}

with

source as (

    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNT_V2') }}

),

renamed as (

    select
        -- Primary Identifiers
        id::varchar as account_id,
        accountv2identity::int as account_v2_identity,
        nid::int as n_id,

        -- Account Information
        code::varchar as account_code,
        name::varchar as account_name,
        fullname::varchar as account_full_name,
        mainaccount::varchar as main_account,
        subaccount::varchar as sub_account,

        -- Sorting and Organization
        accountsort::varchar as account_sort,
        keysort::varchar as key_sort,

        -- Account Classification
        accounttypeid::int as account_type_id,
        accountsubtypeid::int as account_subtype_id,

        -- Account Properties (Booleans)
        active::boolean as is_active,
        normallydebit::boolean as is_normally_debit,
        summarizeinglreports::boolean as is_summarize_in_gl_reports,
        summarizeinjibinvoice::boolean as is_summarize_in_jib_invoice,
        nullif(jibsummaryid, '')::varchar as jib_summary_id,
        bankreconperformed::boolean as is_bank_recon_performed,
        changedsinceprinted::int as changed_since_printed,
        generatefixedassetcandidates::boolean as is_generate_fixed_asset_candidates,
        remeasureatcurrent::boolean as is_remeasure_at_current,
        translateatcurrent::boolean as is_translate_at_current,

        -- Usage Types
        afeusagetypeid::int as afe_usage_type_id,
        alternatereferenceusagetypeid::int as alternate_reference_usage_type_id,
        entryclassificationusagetypeid::int as entry_classification_usage_type_id,
        expensedistributionusagetypeid::int as expense_distribution_usage_type_id,
        manualentryusagetypeid::int as manual_entry_usage_type_id,
        wellusagetypeid::int as well_usage_type_id,

        -- Additional Classifications
        locationgroupingid::int as location_grouping_id,
        lastchangecode::int as last_change_code,

        -- Source System Timestamps
        createdate::timestamp_ntz as created_at,
        updatedate::timestamp_ntz as updated_at,

        -- CDC Metadata
        flow_published_at::timestamp_ntz as flow_published_at

    from source

),

filtered as (

    select * from renamed
    where account_id is not null

),

enhanced as (

    select
        *,
        -- Derived fields
        lower(account_name) like '%accru%' as is_accrual,
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        account_id,
        account_v2_identity,
        n_id,
        account_code,
        account_name,
        account_full_name,
        main_account,
        sub_account,
        account_sort,
        key_sort,
        account_type_id,
        account_subtype_id,
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
        afe_usage_type_id,
        alternate_reference_usage_type_id,
        entry_classification_usage_type_id,
        expense_distribution_usage_type_id,
        manual_entry_usage_type_id,
        well_usage_type_id,
        location_grouping_id,
        last_change_code,
        created_at,
        updated_at,
        flow_published_at,
        _loaded_at

    from enhanced

)

select * from final