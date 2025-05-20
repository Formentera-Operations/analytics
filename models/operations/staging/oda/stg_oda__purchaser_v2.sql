with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_PURCHASER_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Entity relationship
        ENTITYID as entity_id,
        
        -- Configuration and status
        ACTIVE as active,
        BYWELLREVENUERECEIVABLE as by_well_revenue_receivable,
        CDEXCODE as cdex_code,
        PURCHASERV2IDENTITY as purchaser_v2_identity,
        
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