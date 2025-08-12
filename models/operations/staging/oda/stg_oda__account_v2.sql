with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNT_V2') }}

),

renamed as (

    select
        -- Primary Identifiers
        ID as id,
        ACCOUNTV2IDENTITY as account_v2_identity,
        NID as n_id,
        
        -- Account Information
        CODE as code,
        NAME as name,
        FULLNAME as full_name,
        MAINACCOUNT as main_account,
        SUBACCOUNT as sub_account,
        
        -- Sorting and Organization
        ACCOUNTSORT as account_sort,
        KEYSORT as key_sort,
        
        -- Account Classification
        ACCOUNTTYPEID as account_type_id,
        ACCOUNTSUBTYPEID as account_subtype_id,
        
        -- Account Properties
        cast(ACTIVE as boolean) as active,
        cast(NORMALLYDEBIT as boolean) as normally_debit,
        cast(SUMMARIZEINGLREPORTS as boolean) as summarize_in_gl_reports,
        cast(SUMMARIZEINJIBINVOICE as boolean) as summarize_in_jib_invoice,
        JIBSUMMARYID as jib_summary_id,
        cast(BANKRECONPERFORMED as boolean) as bank_recon_performed,
        cast(CHANGEDSINCEPRINTED as boolean) as changed_since_printed,
        cast(GENERATEFIXEDASSETCANDIDATES as boolean) as generate_fixed_asset_candidates,
        cast(REMEASUREATCURRENT as boolean) as remeasure_at_current,
        cast(TRANSLATEATCURRENT as boolean) as translate_at_current,
        
        -- Usage Types
        AFEUSAGETYPEID as afe_usage_type_id,
        ALTERNATEREFERENCEUSAGETYPEID as alternate_reference_usage_type_id,
        ENTRYCLASSIFICATIONUSAGETYPEID as entry_classification_usage_type_id,
        EXPENSEDISTRIBUTIONUSAGETYPEID as expense_distribution_usage_type_id,
        MANUALENTRYUSAGETYPEID as manual_entry_usage_type_id,
        WELLUSAGETYPEID as well_usage_type_id,
        
        -- Additional Classifications
        LOCATIONGROUPINGID as location_grouping_id,
        LASTCHANGECODE as last_change_code,
        
        -- Metadata and Audit Fields
        CREATEDATE as create_date,
        CREATEEVENTID as create_event_id,
        UPDATEDATE as update_date,
        UPDATEEVENTID as update_event_id,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Raw JSON Document
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed