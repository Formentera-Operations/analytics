with source as (

    select * from fo_raw_db."oda"."Account"

),

renamed as (

    select

        -- Identifiers
        "Id" as account_id,
        "AccountIdentity" as account_identity,
        "Code" as account_code,
        "SubAccount" as sub_account,
        "FullName" as full_name,
        "Name" as account_name,
        "MainAccount" as main_account,

        -- Classification
        "TypeName" as account_type_name,
        "AccountTypeId" as account_type_id,
        "SubTypeName" as account_subtype_name,
        "AccountSubTypeId" as account_subtype_id,
        "NormallyDebit" as normally_debit,

        -- Usage Type References
        "AfeUsageTypeId" as afe_usage_type_id,
        "AlternateReferenceUsageTypeId" as alternate_reference_usage_type_id,
        "ManualEntryUsageTypeId" as manual_entry_usage_type_id,
        "WellUsageTypeId" as well_usage_type_id,
        "EntryClassificationUsageTypeId" as entry_classification_usage_type_id,
        "ExpenseDistributionUsageTypeId" as expense_distribution_usage_type_id,

        -- Grouping & Reporting
        "LocationGroupingName" as location_grouping_name,
        "LocationGroupingId" as location_grouping_id,
        "SummarizeinJIBInvoice" as summarize_in_jib_invoice,
        "SummarizeinGLReports" as summarize_in_gl_reports,

        -- Sorting & Flags
        "AccountSort" as account_sort,
        "KeySort" as key_sort,
        "Active" as is_active,
        "BankReconPerformed" as bank_recon_performed,
        "GenerateFixedAssetCandidates" as generate_fixed_asset_candidates,
        "ChangedSincePrinted" as changed_since_printed,
        "LastChangeCode" as last_change_code,
        "TranslateAtCurrent" as translate_at_current,
        "RemeasureAtCurrent" as remeasure_at_current,
        "JIBSummaryID" as jib_summary_id,

        -- Events & Audit Trail
        "CreateEventId" as create_event_id,
        "UpdateEventId" as update_event_id,
        "CreateDate" as create_date,
        "UpdateDate" as update_date,
        "RecordInsertDate" as record_insert_date,
        "RecordUpdateDate" as record_update_date,
        "NId" as n_id,

        -- Fivetran metadata
        "_fivetran_deleted" as is_deleted,
        "_fivetran_synced" as _fivetran_synced,

        -- Optional for snapshots
        "_fivetran_synced" as dbt_valid_from

    from source

)

select * from renamed