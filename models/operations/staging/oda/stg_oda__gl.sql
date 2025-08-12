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
        
        -- Date information (with explicit casting)
        CAST(JOURNALDATE as DATE) as journal_date,
        CAST(ACCRUALDATE as DATE) as accrual_date,
        ACCRUALDATEKEY as accrual_date_key,
        CAST(CASHDATE as DATE) as cash_date,
        CASHDATEKEY as cash_date_key,
        
        -- Description and reference
        CASE 
            WHEN DESCRIPTION = '.' THEN 'No Description'
            WHEN DESCRIPTION IS NULL THEN 'No Description'
            WHEN TRIM(DESCRIPTION) = '' THEN 'No Description'
            ELSE TRIM(DESCRIPTION)
        END as description,
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
        
        -- Financial values (with explicit decimal casting)
        CAST(COALESCE(GROSSVALUE, 0) as DECIMAL(19,4)) as gross_value,
        CAST(COALESCE(GROSSVOLUME, 0) as DECIMAL(19,4)) as gross_volume,
        CAST(COALESCE(NETVALUE, 0) as DECIMAL(19,4)) as net_value,
        CAST(COALESCE(NETVOLUME, 0) as DECIMAL(19,4)) as net_volume,
        
        -- Currency information
        COALESCE(CURRENCYID, 'USD') as currency_id,  -- Default to USD
        CONVERTCURRENCY as convert_currency,
        CONVERTCURRENCYTYPEID as convert_currency_type_id,
        EXCHANGERATEID as exchange_rate_id,
        FLUCTUATIONTYPEID as fluctuation_type_id,
        
        -- Status flags (converted to boolean)
        CASE 
            WHEN CONVERTCURRENCY = TRUE THEN TRUE
            WHEN CONVERTCURRENCY = FALSE THEN FALSE
            ELSE FALSE
        END as is_convert_currency,
        CASE 
            WHEN POSTED = 1 THEN TRUE 
            WHEN POSTED = 0 THEN FALSE
            ELSE FALSE 
        END as is_posted,
        CASE 
            WHEN GENERATEDENTRY = 1 THEN TRUE 
            WHEN GENERATEDENTRY = 0 THEN FALSE
            ELSE FALSE 
        END as is_generated_entry,
        CASE 
            WHEN RECONCILED = 1 THEN TRUE 
            WHEN RECONCILED = 0 THEN FALSE
            ELSE FALSE 
        END as is_reconciled,
        CASE 
            WHEN RECONCILEDTRIAL = 1 THEN TRUE 
            WHEN RECONCILEDTRIAL = 0 THEN FALSE
            ELSE FALSE 
        END as is_reconciled_trial,
        RECONCILEDTYPECODE as reconciled_type_code,
        RECONCILIATIONTYPEID as reconciliation_type_id,
        CASE 
            WHEN PRESENTINJOURNALBALANCE = 1 THEN TRUE 
            WHEN PRESENTINJOURNALBALANCE = 0 THEN FALSE
            ELSE FALSE 
        END as is_present_in_journal_balance,
        CASE 
            WHEN PRESENTINCASHBALANCE = 1 THEN TRUE 
            WHEN PRESENTINCASHBALANCE = 0 THEN FALSE
            ELSE FALSE 
        END as is_present_in_cash_balance,
        CASE 
            WHEN PRESENTINACCRUALBALANCE = 1 THEN TRUE 
            WHEN PRESENTINACCRUALBALANCE = 0 THEN FALSE
            ELSE FALSE 
        END as is_present_in_accrual_balance,
        CASE 
            WHEN INCLUDEINJOURNALREPORT = 1 THEN TRUE 
            WHEN INCLUDEINJOURNALREPORT = 0 THEN FALSE
            ELSE TRUE  -- Default to include
        END as is_include_in_journal_report,
        CASE 
            WHEN INCLUDEINCASHREPORT = 1 THEN TRUE 
            WHEN INCLUDEINCASHREPORT = 0 THEN FALSE
            ELSE TRUE  -- Default to include
        END as is_include_in_cash_report,
        CASE 
            WHEN INCLUDEINACCRUALREPORT = 1 THEN TRUE 
            WHEN INCLUDEINACCRUALREPORT = 0 THEN FALSE
            ELSE TRUE  -- Default to include
        END as is_include_in_accrual_report,
        
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
        TRIM(PAYMENTTYPECODE) as payment_type_code,  -- Remove trailing spaces
        MANUALENTRYREFERENCETYPEID as manual_entry_reference_type_id,
        
        -- Allocation information (with boolean conversion)
        CASE 
            WHEN ISALLOCATIONPARENT = 1 THEN TRUE 
            WHEN ISALLOCATIONPARENT = 0 THEN FALSE
            ELSE FALSE 
        END as is_allocation_parent,
        CASE 
            WHEN ISALLOCATIONGENERATED = 1 THEN TRUE 
            WHEN ISALLOCATIONGENERATED = 0 THEN FALSE
            ELSE FALSE 
        END as is_allocation_generated,
        ALLOCATIONPARENTID as allocation_parent_id,
        ENTRYGROUP as entry_group,
        CAST(ORDINAL as INT) as ordinal,
        
        -- Metadata and timestamps (with explicit timestamp casting)
        CAST(CREATEDATE as TIMESTAMP) as create_date,
        CREATEEVENTID as create_event_id,
        CAST(UPDATEDATE as TIMESTAMP) as update_date,
        UPDATEEVENTID as update_event_id,
        CAST(RECORDINSERTDATE as TIMESTAMP) as record_insert_date,
        CAST(RECORDUPDATEDATE as TIMESTAMP) as record_update_date,
        "_meta/op" as operation_type,
        CAST(FLOW_PUBLISHED_AT as TIMESTAMP) as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
where operation_type <> 'd' --remove soft deletes