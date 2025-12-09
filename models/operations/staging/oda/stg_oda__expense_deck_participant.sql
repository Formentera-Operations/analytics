with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECKPARTICIPANT') }}

),

renamed as (

    select
        -- Primary key and deck identifiers
        "_meta/row_id" as row_id,
        ID as id,
        DECKREVISIONID as deck_revision_id,
        EXPENSEDECKPARTICIPANTIDENTITY as expense_deck_participant_identity,
        
        -- Entity details
        ENTITYTYPEID as entity_type_id,
        COMPANYID as company_id,
        OWNERID as owner_id,
        
        -- Interest information
        INTERESTTYPEID as interest_type_id,
        DECIMALINTEREST as decimal_interest,
        CUSTOMINTERESTTYPEID as custom_interest_type_id,

        --Memo
        ISMEMO as is_memo,
        MEMOCOMPANYID as memo_company_id, 
        
        -- Metadata and timestamps
        CREATEDATE as create_date,
        CREATEEVENTID as create_event_id,
        UPDATEDATE as update_date,
        UPDATEEVENTID as update_event_id,
        "_meta/op" as operation_type,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed