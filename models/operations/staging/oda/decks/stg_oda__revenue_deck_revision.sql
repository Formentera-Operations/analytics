with source as (

    select * from {{ source('oda', 'ODA_REVENUEDECKREVISION') }}

),

renamed as (

    select
        -- Primary key
        ID as id,

        -- Revenue deck relationship
        DECKID as deck_id,
        NAME as name,

        -- Revision details
        REVISIONNUMBER as revision_number,
        REVISIONSTATEID as revision_state_id,
        REVENUEDECKREVISIONIDENTITY as revenue_deck_revision_identity,
        CHANGENOTE as change_note,
        IMPORTDATAID as import_data_id,

        -- Interest calculations
        TOTALINTERESTEXPECTED as total_interest_expected,
        NRIACTUAL as nri_actual,

        -- Status and close information
        CLOSEDATE as close_date,
        CLOSEBYUSERID as close_by_user_id,

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
