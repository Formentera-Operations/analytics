with source as (
    
    select * from {{ source('oda', 'ODA_REVENUEDECKPARTICIPANT') }}

),

renamed as (

    select
        -- Primary key and deck identifiers
        ID as id,
        DECKREVISIONID as deck_revision_id,
        REVENUEDECKPARTICIPANTIDENTITY as revenue_deck_participant_identity,
        ISMEMO as is_memo,
        
        -- Entity details
        ENTITYTYPEID as entity_type_id,
        COMPANYID as company_id,
        OWNERID as owner_id,
        
        -- Interest and Suspense information
        INTERESTTYPEID as interest_type_id,
        DECIMALINTEREST as decimal_interest,
        CUSTOMINTERESTTYPEID as custom_interest_type_id,
        AUTOSUSPENDPAYMENT as auto_suspend_payment,
        SUSPENSECATEGORYID as suspend_category_id, 
        
        -- Metadata and timestamps
        CREATEDATE as create_date,
        CREATEEVENTID as create_event_id,
        UPDATEDATE as update_date,
        UPDATEEVENTID as update_event_id,
        RECORDINSERTDATE as record_insert_date,  
        RECORDUPDATEDATE as record_update_date, 
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed