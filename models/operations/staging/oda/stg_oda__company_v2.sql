with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_COMPANY_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Company identifiers
        CODE as code,
        CODESORT as code_sort,
        NAME as name,
        FULLNAME as full_name,
        COMMENT as comment,
        COMPANYV2IDENTITY as company_v2_identity,
        
        -- Tax information
        TAXID as tax_id,
        TAXIDTYPEID as tax_id_type_id,
        ISPARTNERSHIP as is_partnership,
        K1TYPEID as k1_type_id,
        
        -- Contact information
        MAINCONTACTID as main_contact_id,
        URL as url,
        
        -- Fiscal/Accounting configuration
        BASISID as basis_id,
        FISCALYEARENDMONTH as fiscal_year_end_month,
        CURRENTFISCALYEAR as current_fiscal_year,
        
        -- Current periods
        CURRENTAPMONTH as current_ap_month,
        CURRENTARMONTH as current_ar_month,
        CURRENTGLMONTH as current_gl_month,
        CURRENTJIBMONTH as current_jib_month,
        CURRENTREVENUEMONTH as current_revenue_month,
        
        -- AR/AP configuration
        ARINVOICESEQUENCEID as ar_invoice_sequence_id,
        APINVOICESEQUENCEID as ap_invoice_sequence_id,
        ARSTATEMENTPRINTPENDING as ar_statement_print_pending,
        ACCEPTMEMOENTRIES as accept_memo_entries,
        ACHID as ach_id,
        ISACHCORPORATION as is_ach_corporation,
        
        -- Default settings
        DEFAULTACCRUALITEMSID as default_accrual_items_id,
        DEFAULTEXPENSEINTERESTTYPEID as default_expense_interest_type_id,
        DEFAULTEXPENSECUSTOMINTERESTTYPEID as default_expense_custom_interest_type_id,
        DEFAULTEXPENSEENTITLEMENTINTEREST as default_expense_entitlement_interest,
        DEFAULTREVENUEINTERESTTYPEID as default_revenue_interest_type_id,
        DEFAULTREVENUECUSTOMINTERESTTYPEID as default_revenue_custom_interest_type_id,
        DEFAULTREVENUEENTITLEMENTINTEREST as default_revenue_entitlement_interest,
        
        -- Retention configuration
        APDETAILRETENTIONMONTHS as ap_detail_retention_months,
        ARDETAILRETENTIONMONTHS as ar_detail_retention_months,
        GLDETAILRETENTIONMONTHS as gl_detail_retention_months,
        GLBALANCERETENTIONDATE as gl_balance_retention_date,
        GLBALANCERETENTIONYEARS as gl_balance_retention_years,
        JIBDETAILRETENTIONYEARS as jib_detail_retention_years,
        REVENUEDETAILRETENTIONMONTHS as revenue_detail_retention_months,
        
        -- Other fields
        CDEXCODE as cdex_code,
        NID as n_id,
        INACTIVEDATE as inactive_date,
        SUMMARIZERCHUNKSIZE as summarizer_chunk_size,
        TENANTID as tenant_id,
        
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