with source as (

    select * from {{ source('oda', 'ODA_USERFIELD') }}

),

renamed as (
    select
        USERFIELDIDENTITY as "UserFieldIdentity",
        ID as "Id",
        ENTITYCODE as "EntityCode",
        ENTITYNAME as "EntityName",
        ENTITYTYPEID as "EntityTypeId",
        RECORDINSERTDATE as "RecordInsertDate",
        RECORDUPDATEDATE as "RecordUpdateDate",
        USERFIELDNAME as "UserFieldName",
        USERFIELDVALUESTRING as "UserFieldValueString",
        FLOW_PUBLISHED_AT,
        FLOW_DOCUMENT
    from source
)

select * from renamed
