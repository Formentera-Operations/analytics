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
        JOURNALDATE as journal_date,
        ACCRUALDATE as accrual_date,
        ACCRUALDATEKEY as accrual_date_key,
        CASHDATE as cash_date,
        CASHDATEKEY as cash_date_key,
        
        -- Description and reference
        DESCRIPTION as description,
        REFERENCE as reference,
        
        -- Source information
        SOURCEMODULE as source_module,
        SOURCEMODULEID as source_module_id,
        SOURCEMODULECODE as source_module_code,
        SOURCEMODULENAME as source_module_name,
        
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
        GROSSVALUE as gross_value,
        GROSSVOLUME as gross_volume,
        NETVALUE as net_value,
        NETVOLUME as net_volume,
        
        -- Currency information
        CURRENCYID as currency_id,
        CONVERTCURRENCY as convert_currency,
        CONVERTCURRENCYTYPEID as convert_currency_type_id,
        EXCHANGERATEID as exchange_rate_id,
        FLUCTUATIONTYPEID as fluctuation_type_id,
        
        -- Status flags
        POSTED as posted,
        GENERATEDENTRY as generated_entry,
        RECONCILED as reconciled,
        RECONCILEDTRIAL as reconciled_trial,
        RECONCILEDTYPECODE as reconciled_type_code,
        RECONCILIATIONTYPEID as reconciliation_type_id,
        PRESENTINJOURNALBALANCE as present_in_journal_balance,
        PRESENTINCASHBALANCE as present_in_cash_balance,
        PRESENTINACCRUALBALANCE as present_in_accrual_balance,
        INCLUDEINJOURNALREPORT as include_in_journal_report,
        INCLUDEINCASHREPORT as include_in_cash_report,
        INCLUDEINACCRUALREPORT as include_in_accrual_report,
        
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
        PAYMENTTYPECODE as payment_type_code,
        MANUALENTRYREFERENCETYPEID as manual_entry_reference_type_id,
        
        -- Allocation information
        ISALLOCATIONPARENT as is_allocation_parent,
        ISALLOCATIONGENERATED as is_allocation_generated,
        ALLOCATIONPARENTID as allocation_parent_id,
        ENTRYGROUP as entry_group,
        ORDINAL as ordinal,
        
        -- Metadata and timestamps
        CREATEDATE as create_date,
        CREATEEVENTID as create_event_id,
        UPDATEDATE as update_date,
        UPDATEEVENTID as update_event_id,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        "_meta/op" as operation_type,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed