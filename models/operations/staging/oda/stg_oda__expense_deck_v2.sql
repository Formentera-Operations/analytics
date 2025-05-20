with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECK_V2') }}

),

renamed as (

    select
        -- Primary keys
        ID as id,
        DECKSETID as deck_set_id,
        EFFECTIVEDATE as effective_date,
        
        -- Other identifiers
        EXPENSEDECKIDENTITY as expense_deck_identity,
        NID as n_id,
        
        -- Metadata and timestamps
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        UPDATEDATE as update_date,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed