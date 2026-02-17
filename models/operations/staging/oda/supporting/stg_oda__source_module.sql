with source as (

    select * from {{ source('oda', 'ODA_BATCH_ODA_SOURCEMODULE') }}

),

renamed as (

    select
        -- Primary key
        ID as id,

        -- Module identifiers
        CODE as code,
        NAME as name,
        FULLNAME as full_name,
        MODULE as module,
        SOURCEMODULEIDENTITY as source_module_identity,

        -- Metadata and timestamps
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,

        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
