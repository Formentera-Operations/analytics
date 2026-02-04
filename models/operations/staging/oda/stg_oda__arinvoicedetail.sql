with source as (
    select * from {{ source('oda', 'ODA_ARINVOICEDETAIL') }}
),

renamed as (
    select
        -- ids
        ID                          AS id,
        ARINVOICEDETAILIDENTITY     AS ar_invoice_detail_identity,
        ACCOUNTID                   AS account_id,
        INVOICEID                   AS invoice_id,
        DISTRIBUTIONCOMPANYID       AS distribution_company_id,
        
        --numerics
        DISTRIBUTIONAMOUNT          AS distribution_amount,
        NETVOLUME                   AS net_volume,

        --strings
        DESCRIPTION                 AS description,
        ORDINAL                     AS ordinal,

        -- timestamps and events
        CREATEDATE                  AS create_date,
        CREATEEVENTID               AS create_event_id,
        RECORDINSERTDATE            AS record_insert_date,
        RECORDUPDATEDATE            AS record_update_date,
        UPDATEDATE                  AS update_date,
        UPDATEEVENTID               AS update_event_id,

        -- metadata
       --_meta_op,
        FLOW_PUBLISHED_AT           AS flow_published_at,
        FLOW_DOCUMENT               AS flow_document

    from source
)

select * from renamed