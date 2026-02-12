with source as (

    select * from {{ source('oda', 'ODA_REVISIONSTATE') }}

),

renamed as (

    select
        -- Primary key
        ID as id,

        -- Revision state identifiers
        NAME as name,
        FULLNAME as full_name,
        REVISIONSTATEIDENTITY as revision_state_identity,

        -- Metadata and timestamps
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,

        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
