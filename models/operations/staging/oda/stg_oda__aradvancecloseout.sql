with source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARADVANCECLOSEOUT') }}
),

renamed as (
    select
    -- IDs
    ID                              AS id,
    ARADVANCECLOSEOUTIDENTITY       AS ar_advance_closeout_identity,
    VOUCHERID                       AS voucher_id,
    WELLID                          AS well_id,
    TARGETVOUCHERID                 AS target_voucher_id,
    CREATEEVENTID                   AS create_event_id,
    UPDATEEVENTID                   AS update_event_id,

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