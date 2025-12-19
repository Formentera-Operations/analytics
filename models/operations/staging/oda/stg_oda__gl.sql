{{
    config(
        materialized='view',
        tags=['staging', 'oda', 'gl']
    )
}}

{#
    Staging model for ODA General Ledger
    
    Source: ODA GL table (Estuary CDC)
    Grain: One row per GL entry (id)
    
    Notes:
    - Soft deletes filtered out (operation_type = 'd')
    - Currency defaults to USD when null (tracked via is_currency_defaulted)
    - Financial values default to 0 when null
#}

with source as (
    
    select * from {{ source('oda', 'GL') }}

),

renamed as (

    select
        -- Primary keys
        ID as id,
        COMPANYID as company_id,
        ACCOUNTID as account_id,
        JOURNALDATEKEY as journal_date_key,
        
        -- GL identity
        GLIDENTITY as gl_identity,
        NID as n_id,
        
       -- Date information
        date(convert_timezone('UTC', JOURNALDATE)) as journal_date,
        date(convert_timezone('UTC', ACCRUALDATE)) as accrual_date,
        ACCRUALDATEKEY as accrual_date_key,
        date(convert_timezone('UTC', CASHDATE)) as cash_date,
        CASHDATEKEY as cash_date_key,
        
        -- Description and reference
        CASE 
            WHEN TRIM(DESCRIPTION) IN ('.', '') OR DESCRIPTION IS NULL THEN 'No Description'
            ELSE TRIM(DESCRIPTION)
        END as description,
        TRIM(REFERENCE) as reference,
        
        -- Source information
        TRIM(SOURCEMODULE) as source_module,
        SOURCEMODULEID as source_module_id,
        TRIM(SOURCEMODULECODE) as source_module_code,
        TRIM(SOURCEMODULENAME) as source_module_name,
        
        -- Related entities
        AFEID as afe_id,
        WELLID as well_id,
        ENTITYID as entity_id,
        ENTITYCOMPANYID as entity_company_id,
        ENTITYOWNERID as entity_owner_id,
        ENTITYPURCHASERID as entity_purchaser_id,
        ENTITYVENDORID as entity_vendor_id,
        
        -- Location information
        LOCATIONTYPE as location_type,
        LOCATIONCOMPANYID as location_company_id,
        LOCATIONOWNERID as location_owner_id,
        LOCATIONPURCHASERID as location_purchaser_id,
        LOCATIONVENDORID as location_vendor_id,
        LOCATIONWELLID as location_well_id,
        
        -- Financial values
        CAST(COALESCE(GROSSVALUE, 0) as DECIMAL(19,4)) as gross_value,
        CAST(COALESCE(GROSSVOLUME, 0) as DECIMAL(19,4)) as gross_volume,
        CAST(COALESCE(NETVALUE, 0) as DECIMAL(19,4)) as net_value,
        CAST(COALESCE(NETVOLUME, 0) as DECIMAL(19,4)) as net_volume,
        
        -- Currency information
        CURRENCYID as currency_id,
        CURRENCYID IS NULL as is_currency_missing,
        CONVERTCURRENCYTYPEID as convert_currency_type_id,
        EXCHANGERATEID as exchange_rate_id,
        FLUCTUATIONTYPEID as fluctuation_type_id,
        
        -- Status flags
        COALESCE(CONVERTCURRENCY, FALSE) as is_convert_currency,
        COALESCE(POSTED = 1, FALSE) as is_posted,
        COALESCE(GENERATEDENTRY = 1, FALSE) as is_generated_entry,
        COALESCE(RECONCILED = 1, FALSE) as is_reconciled,
        COALESCE(RECONCILEDTRIAL = 1, FALSE) as is_reconciled_trial,
        TRIM(RECONCILEDTYPECODE) as reconciled_type_code,
        RECONCILIATIONTYPEID as reconciliation_type_id,
        COALESCE(PRESENTINJOURNALBALANCE = 1, FALSE) as is_present_in_journal_balance,
        COALESCE(PRESENTINCASHBALANCE = 1, FALSE) as is_present_in_cash_balance,
        COALESCE(PRESENTINACCRUALBALANCE = 1, FALSE) as is_present_in_accrual_balance,
        COALESCE(INCLUDEINJOURNALREPORT = 1, TRUE) as is_include_in_journal_report,
        COALESCE(INCLUDEINCASHREPORT = 1, TRUE) as is_include_in_cash_report,
        COALESCE(INCLUDEINACCRUALREPORT = 1, TRUE) as is_include_in_accrual_report,
        
        -- Related documents
        VOUCHERID as voucher_id,
        APINVOICEID as ap_invoice_id,
        APCHECKID as ap_check_id,
        ARINVOICEID as ar_invoice_id,
        CHECKREVENUEID as check_revenue_id,
        PURCHASERREVENUERECEIPTID as purchaser_revenue_receipt_id,
        PENDINGEXPENSEDECKID as pending_expense_deck_id,
        PENDINGEXPENSEDECKSETID as pending_expense_deck_set_id,
        SOURCEEXPENSEDECKREVISIONID as source_expense_deck_revision_id,
        SOURCEREVENUEDECKREVISIONID as source_revenue_deck_revision_id,
        SOURCEWELLALLOCATIONDECKID as source_well_allocation_deck_id,
        SOURCEWELLALLOCATIONDECKREVISIONID as source_well_allocation_deck_revision_id,
        
        -- Payment information
        PAYMENTTYPEID as payment_type_id,
        TRIM(PAYMENTTYPECODE) as payment_type_code,
        MANUALENTRYREFERENCETYPEID as manual_entry_reference_type_id,
        
        -- Allocation information
        COALESCE(ISALLOCATIONPARENT = 1, FALSE) as is_allocation_parent,
        COALESCE(ISALLOCATIONGENERATED = 1, FALSE) as is_allocation_generated,
        ALLOCATIONPARENTID as allocation_parent_id,
        ENTRYGROUP as entry_group,
        CAST(ORDINAL as INT) as ordinal,
        
        -- Source metadata
        "_meta/op" as _operation_type,
        CAST(FLOW_PUBLISHED_AT as TIMESTAMP) as _flow_published_at,
        CAST(RECORDINSERTDATE as TIMESTAMP) as _record_insert_date,
        CAST(RECORDUPDATEDATE as TIMESTAMP) as _record_update_date,
        
        -- Audit timestamps
        CAST(CREATEDATE as TIMESTAMP) as created_at,
        CREATEEVENTID as create_event_id,
        CAST(UPDATEDATE as TIMESTAMP) as updated_at,
        UPDATEEVENTID as update_event_id,
        CURRENT_TIMESTAMP() as _loaded_at

        {# 
        Excluding FLOW_DOCUMENT to reduce table size.
        If needed for debugging, consider a separate stg_oda__gl_raw model.
        
        FLOW_DOCUMENT as _flow_document
        #}

    from source

)

select * from renamed

-- Exclude soft-deleted records from CDC stream
where _operation_type != 'd'
