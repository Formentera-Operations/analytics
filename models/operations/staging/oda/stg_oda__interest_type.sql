with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_INTERESTTYPE') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Interest identifiers
        CODE as code,
        NAME as name,
        FULLNAME as full_name,
        INTERESTTYPEIDENTITY as interest_identity,
        
        -- Interest information
        CDEXCODE as cdex_code, 
       
        
        -- Metadata and timestamps
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
