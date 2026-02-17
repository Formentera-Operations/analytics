with source as (

    select * from {{ source('oda', 'ODA_BATCH_ODA_REVENUEDECK_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,

        -- Related entities and identifiers
        DECKSETID as deck_set_id,
        DECKTYPEID as deck_type_id,
        REVENUEDECKIDENTITY as revenue_deck_identity,

        -- Date information
        EFFECTIVEDATE as effective_date,

        -- Metadata and timestamps
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,

        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
