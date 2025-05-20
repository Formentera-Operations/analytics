with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_VENDOR_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Entity relationship
        ENTITYID as entity_id,
        VENDORV2IDENTITY as vendor_v2_identity,
        
        -- Status and configuration
        ACTIVE as active,
        CHANGEDSINCEPRINTED as changed_since_printed,
        HOLDAPCHECKS as hold_ap_checks,
        PRINT1099 as print_1099,
        SECONDTINNOTICESENT as second_tin_notice_sent,
        
        -- AP configuration
        APDUEDATECALCULATEDBASEDON as ap_due_date_calculated_based_on,
        APCHECKSTUBREFERENCE as ap_check_stub_reference,
        APPAYMENTTYPEID as ap_payment_type_id,
        ONEINVOICEPERCHECK as one_invoice_per_check,
        TAKEALLDISCOUNTS as take_all_discounts,
        TERMS as terms,
        
        -- Default accounts and currencies
        DEFAULTAPCURRENCYID as default_ap_currency_id,
        DEFAULTEXPENSEACCOUNTID as default_expense_account_id,
        MINIMUMAPCHECK as minimum_ap_check,
        MINIMUMCURRENCYID as minimum_currency_id,
        
        -- Other fields
        NID as n_id,
        
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