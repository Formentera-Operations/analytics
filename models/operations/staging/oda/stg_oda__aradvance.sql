with source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARADVANCE') }}
),

renamed as (
    select
    -- IDs
    ID                              AS id,
    ARADVANCEIDENTITY               AS ar_advance_identity,
    AFEID                           AS afe_id,
    VOUCHERID                       AS voucher_id,
    WELLID                          AS well_id,
    COMPANYID                       AS company_id, 
    CURRENCYID                      AS currency_id, 
    EXPENSEDECKID                   AS expense_deck_id, 
    CREATEEVENTID                   AS create_event_id,
    UPDATEEVENTID                   AS update_event_id,

    -- Dates
    ADVANCEDATE                     AS advance_date, 

    -- Numeric Values
    GROSSAMOUNT                     AS gross_amount,
    NETAMOUNT                       AS net_amount,
    EXPENSEDECKINTERESTTOTAL        AS expense_deck_interest_total,  
    
    -- Strings
    DESCRIPTION                     AS description,

    -- Booleans
    POSTED                          AS posted,
    ISAFTERCASING                   AS is_after_casing, 


    -- Timestamps
    CREATEDATE                      AS create_date,
    UPDATEDATE                      AS update_date,
    RECORDINSERTDATE                AS record_insert_date,
    RECORDUPDATEDATE                AS record_update_date,

    -- Metadata
    --"_meta/op"                      AS _meta_op,
    FLOW_PUBLISHED_AT               AS flow_published_at,
    FLOW_DOCUMENT                   AS flow_document


    from source
)

select * from renamed