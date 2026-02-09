with source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARINVOICEADJUSTMENTDETAIL') }}
),

renamed as (
    select
    -- IDs
    ID                              AS id,
    INVOICEADJUSTMENTID             AS invoice_adjustment_id,
    ARINVOICEADJUSTMENTDETAILIDENTITY AS ar_invoice_adjustment_detail_identity,
    INVOICEID                       AS invoice_id,
    AFEID                           AS afe_id,
    CREATEEVENTID                   AS create_event_id, 
    UPDATEEVENTID                   AS update_event_id,
 

    -- numerics
    ADJUSTMENTDETAILAMOUNT          AS adjustment_detail_amount,
    NETVOLUME                       AS net_volume,

    -- strings
    DESCRIPTION                     as description,

    -- Timestamps
    CREATEDATE                      AS create_date,
    UPDATEDATE                      AS update_date,
    RECORDINSERTDATE                AS record_insert_date,
    RECORDUPDATEDATE                AS record_update_date,

    -- Metadata
    FLOW_PUBLISHED_AT               AS flow_published_at,
    FLOW_DOCUMENT                   AS flow_document


    from source
)

select * from renamed