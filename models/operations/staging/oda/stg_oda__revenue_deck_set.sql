with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_REVENUEDECKSET') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Identifying information
        CODE as code,
        CODESORT as code_sort,
        REVENUEDECKSETIDENTITY as revenue_deck_set_identity,
        
        -- Related entities
        COMPANYID as company_id,
        WELLID as well_id,
        PRODUCTID as product_id,
        
        -- Configuration flags
        ISDEFAULTDECK as is_default_deck,
        ISGASENTITLEMENTDECK as is_gas_entitlement_deck,
        
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