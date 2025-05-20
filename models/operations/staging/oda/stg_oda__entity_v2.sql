with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_ENTITY_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Entity identifiers
        CODE as code,
        CODESORT as code_sort,
        NAME as name,
        FULLNAME as full_name,
        ENTITY_V2IDENTITY as entity_v2_identity,
        
        -- Tax information
        TAXID as tax_id,
        TAXIDTYPEID as tax_id_type_id,
        NAME1099 as name_1099,
        
        -- Contact information
        MAINCONTACTID as main_contact_id,
        
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