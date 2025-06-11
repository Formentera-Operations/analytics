with source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNTTYPE') }}
),

renamed as (
    select
        ID as account_type_id,
        CODE as type_code,
        NAME as type_name,
        FULLNAME as type_full_name,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date
    from source
)

select * from renamed