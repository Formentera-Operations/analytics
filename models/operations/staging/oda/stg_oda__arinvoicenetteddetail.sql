with source as (
    select * from {{ source('oda', 'ODA_ARINVOICENETTEDDETAIL') }}
),

renamed as (
    select
    -- IDs
    ID                              AS id,
    ARINVOICENETTEDDETAILIDENTITY   AS ar_invoice_netted_detail_identity,
    INVOICEID                       AS invoice_id,
    OWNERREVENUEDETAILID            AS owner_revenue_detail_id,
    VOUCHERID                       AS voucher_id,
    WELLID                          AS well_id,

    -- Values & Volumes
    NETVALUENONWORKING              AS net_value_non_working, 
    NETVALUEWORKING                 AS net_value_working, 
    NETVOLUMENONWORKING             AS net_volume_non_working, 
    NETVOLUMEWORKING                AS net_volume_working, 
    NETTEDAMOUNT                    AS netted_amount, 

    -- Dates
    ACCRUALDATE                     AS accrual_date,
    NETTINGDATE                     AS netting_date,
 
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