with source as (
    select * from {{ source('oda', 'ODA_ARINVOICEPAYMENTDETAIL') }}
),

renamed as (
    select
        -- ids
        ID                                  AS id,
        ARINVOICEPAYMENTDETAILIDENTITY      AS ar_invoice_payment_detail_identity,
        INVOICEID                           AS invoice_id,
        INVOICEPAYMENTID                    AS invoice_payment_id,
    
        --numerics
        AMOUNTAPPLIED                       AS amount_applied,
        AMOUNTDUEBEFOREPOSTING              AS amount_due_before_posting,

        -- timestamps and events
        CREATEDATE                  AS create_date,
        RECORDINSERTDATE            AS record_insert_date,
        RECORDUPDATEDATE            AS record_update_date,
        UPDATEDATE                  AS update_date,

        -- metadata
       --_meta_op,
        FLOW_PUBLISHED_AT           AS flow_published_at,
        FLOW_DOCUMENT               AS flow_document

    from source
)

select * from renamed