with source as (
    select * from {{ source('oda', 'ODA_AFEBUDGETDETAIL_V2') }}
),

renamed as (
    select
        -- ids
        id,
        afebudgetdetail_v2identity as afe_budget_detail_v2_identity,
        afebudgetentryid as afe_budget_entry_id,
        createeventid as create_event_id,
        updateeventid as update_event_id,

        -- timestamps
        createdate as created_at,
        recordinsertdate as record_inserted_at,
        recordupdatedate as record_updated_at,
        updatedate as updated_at,

        -- numerics
        amount,
        month,

        -- metadata
        "_meta/op" as _meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed
