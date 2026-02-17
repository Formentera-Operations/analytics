with source as (
    select * from {{ source('oda', 'ODA_CHECKREVENUE') }}
),

renamed as (

    select
        -- Primary keys
        ID as id,
        CHECKREVENUEIDENTITY as check_revenue_identity,
        ACCOUNTID as account_id,
        COMPANYID as company_id,
        CURRENCYID as currency_id,
        IMPORTDATAID as import_data_id,
        OWNERID as owner_id,
        PAYMENTTYPEID as payment_type_id,
        VOUCHERID as voucher_id,

        -- Identifiers
        PAYMENTTYPECODE as payment_type_code,
        TRANSACTIONNUMBER as transaction_number,

        -- Amounts
        CHECKAMOUNT as check_amount,

        -- Booleans
        INCLUDEINACCRUALREPORT as include_in_accrual_report,
        RECONCILED as reconciled,
        SYSTEMGENERATED as system_generated,
        VOIDED as voided,

        -- Dates
        ACCRUALDATE as accrual_date,
        GLVOIDDATE as gl_void_date,
        VOIDEDDATE as voided_date,
        VOID1099YEAR as void_1099_year,
        ISSUEDDATE as issued_date,
        ISSUEDDATEKEY as issued_date_key,

        -- Metadata and timestamps
        CREATEDATE as create_date,
        CREATEEVENTID as create_event_id,
        UPDATEDATE as update_date,
        UPDATEEVENTID as update_event_id,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,

        -- Full document JSON for reference
        --"_meta_op",
        FLOW_DOCUMENT as flow_document


    from source

)

select * from renamed
