with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_REVENUESUSPENSECATEGORY') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Module identifiers
        CODE as code,
        NAME as name,
        FULLNAME as full_name,
        REVENUESUSPENSECATEGORYIDENTITY as revemue_suspense_category_identity,
        
        -- Metadata and timestamps
        CREATEDATE as create_date,
        UPDATEDATE as update_date,
        RECORDINSERTDATE as record_insert_date,  
        RECORDUPDATEDATE as record_update_date, 
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed