with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_USERFIELD') }}

),

renamed as (
    select
        "Id", 
        "UserFieldIdentity", 
        "EntityName", 
        "UserFieldValueString", 
        "EntityCode", 
        "EntityTypeId", 
        "RecordInsertDate", 
        "UserFieldName", 
        "RecordUpdateDate", 
        "_fivetran_deleted", 
        "_fivetran_synced"
    from source
)

Select * from source