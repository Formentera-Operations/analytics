with source as (

    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNTTYPE') }}

),

renamed as (

    select
        -- Primary Identifiers
        ID as account_type_id,
        
        -- Type Information
        CODE as type_code,
        NAME as type_name,
        FULLNAME as type_full_name,
        
        -- Metadata and Audit Fields
        cast(RECORDINSERTDATE as timestamp) as record_insert_date,
        cast(RECORDUPDATEDATE as timestamp) as record_update_date

    from source

)

select * from renamed