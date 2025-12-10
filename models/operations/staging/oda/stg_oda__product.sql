with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_PRODUCT') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Product identifiers
        CODE as code,
        CODESORT as code_sort,
        NAME as name,
        FULLNAME as full_name,
        PRODUCTIDENTITY as product_identity,
        
        -- Product information
        TYPEID as type_id,
        CURRENCYID as currency_id,
        LOWESTTYPICALPRICE as lowest_typical_price,
        HIGHESTTYPICALPRICE as highest_typical_price,
        CONVERSIONTOEQUIVALENTBARRELS as conversion_to_equivalent_barrels,
        CDEXCODE as cdex_code, 
       
        
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
