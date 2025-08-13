with source as (

    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNTSUBTYPE') }}

),

renamed as (

    select
        -- Primary Identifiers
        ID as account_subtype_id,
        ACCOUNTTYPEID as account_type_id,
        
        -- Subtype Information
        CODE as subtype_code,
        NAME as subtype_name,
        FULLNAME as subtype_full_name,
        
        -- Properties
        cast(NORMALLYDEBIT as boolean) as normally_debit,
        
        -- Metadata and Audit Fields
        cast(RECORDINSERTDATE as timestamp) as record_insert_date,
        cast(RECORDUPDATEDATE as timestamp) as record_update_date

    from source

)

select * from renamed