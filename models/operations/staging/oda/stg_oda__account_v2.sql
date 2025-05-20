with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNT_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Account identifiers
        CODE as code,
        NAME as name,
        FULLNAME as full_name,
        MAINACCOUNT as main_account,
        SUBACCOUNT as sub_account,
        ACCOUNTSORT as account_sort,
        KEYSORT as key_sort,
        
        -- Account type information
        ACCOUNTTYPEID as account_type_id,
        ACCOUNTSUBTYPEID as account_subtype_id,
        ACCOUNTV2IDENTITY as account_v2_identity,
        
        -- Account properties
        ACTIVE as active,
        NORMALLYDEBIT as normally_debit,
        SUMMARIZEINGLREPORTS as summarize_in_gl_reports,
        SUMMARIZEINJIBINVOICE as summarize_in_jib_invoice,
        JIBSUMMARYID as jib_summary_id,
        BANKRECONPERFORMED as bank_recon_performed,
        CHANGEDSINCEPRINTED as changed_since_printed,
        GENERATEFIXEDASSETCANDIDATES as generate_fixed_asset_candidates,
        REMEASUREATCURRENT as remeasure_at_current,
        TRANSLATEATCURRENT as translate_at_current,
        
        -- Usage type IDs
        AFEUSAGETYPEID as afe_usage_type_id,
        ALTERNATEREFERENCEUSAGETYPEID as alternate_reference_usage_type_id,
        ENTRYCLASSIFICATIONUSAGETYPEID as entry_classification_usage_type_id,
        EXPENSEDISTRIBUTIONUSAGETYPEID as expense_distribution_usage_type_id,
        MANUALENTRYUSAGETYPEID as manual_entry_usage_type_id,
        WELLUSAGETYPEID as well_usage_type_id,
        
        -- Other fields
        LOCATIONGROUPINGID as location_grouping_id,
        NID as n_id,
        LASTCHANGECODE as last_change_code,
        
        -- Metadata and timestamps
        CREATEDATE as create_date,
        CREATEEVENTID as create_event_id,
        UPDATEDATE as update_date,
        UPDATEEVENTID as update_event_id,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed