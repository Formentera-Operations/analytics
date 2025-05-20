with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECKREVISIONVIEW') }}

),

renamed as (

    select
        -- Primary key and identifiers
        "_meta/row_id" as row_id,
        ID as id,
        DECKID as deck_id,
        NAME as name,
        REVISIONNUMBER as revision_number,
        REVISIONSTATEID as revision_state_id,
        
        -- Revision details
        CHANGENOTE as change_note,
        ISAFTERCASING as is_after_casing,
        USEJIBRATEASOFPAYMENT as use_jib_rate_as_of_payment,
        
        -- Interest information
        TOTALINTERESTACTUAL as total_interest_actual,
        TOTALINTERESTEXPECTED as total_interest_expected,
        
        -- Status and dates
        CLOSEDATE as close_date,
        CLOSEBYUSERID as close_by_user_id,
        
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