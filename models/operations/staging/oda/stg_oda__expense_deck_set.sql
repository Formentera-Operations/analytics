with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_EXPENSEDECKSET') }}

),

renamed as (

    select
        -- Primary keys
        ID as id,
        COMPANYID as company_id,
        WELLID as well_id,
        CODE as code,
        
        -- Other identifiers
        CODESORT as code_sort,
        EXPENSEDECKSETIDENTITY as expense_deck_set_identity,
        
        -- Configuration
        ISDEFAULTDECK as is_default_deck,
        
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