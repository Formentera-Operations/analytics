with source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ACCOUNTSUBTYPE') }}
),

renamed as (
    select
        ID as account_subtype_id,
        ACCOUNTTYPEID as account_type_id,
        CODE as subtype_code,
        NAME as subtype_name,
        FULLNAME as subtype_full_name,
        NORMALLYDEBIT as normally_debit,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date
    from source
)

select * from renamed