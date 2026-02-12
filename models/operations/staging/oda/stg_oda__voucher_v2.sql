with source as (

    select * from {{ source('oda', 'ODA_VOUCHER_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,

        -- Voucher identifiers
        CODE as code,
        VOUCHERIDENTITY as voucher_identity,
        VOUCHERTYPEID as voucher_type_id,

        -- Dates and time information
        VOUCHERDATE as voucher_date,
        ACCRUALREVERSALDATE as accrual_reversal_date,
        DATEKEY as date_key,

        -- Description and status
        DESCRIPTION as description,
        DELETED as deleted,
        HISTORICAL as historical,
        POSTED as posted,
        READYTOPOST as ready_to_post,
        ISMANUALJOURNALACCRUAL as is_manual_journal_accrual,

        -- Currency information
        CURRENCYID as currency_id,
        CONVERTCURRENCYTYPEID as convert_currency_type_id,
        EXCHANGERATEID as exchange_rate_id,

        -- Company information
        ORIGINATINGCOMPANYID as originating_company_id,

        -- Financial information
        SUBLEDGERTOTAL as subledger_total,

        -- Import information
        IMPORTDATAID as import_data_id,

        -- Posted information
        POSTEDDATE as posted_date,
        POSTEDBYID as posted_by_id,
        POSTEDBYUSERID as posted_by_user_id,
        POSTEDBYNAME as posted_by_name,

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
