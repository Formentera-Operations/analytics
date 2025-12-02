with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_USERFIELD') }}

),

renamed as (
    select
        USERFIELDIDENTITY AS "UserFieldIdentity"
        ,ID AS "Id"
        ,ENTITYCODE AS "EntityCode"
        ,ENTITYNAME AS "EntityName"
        ,ENTITYTYPEID AS "EntityTypeId"
        ,RECORDINSERTDATE AS "RecordInsertDate"
        ,RECORDUPDATEDATE AS "RecordUpdateDate"
        ,USERFIELDNAME AS "UserFieldName"
        ,USERFIELDVALUESTRING AS "UserFieldValueString"
        ,"_meta/op"
        ,FLOW_PUBLISHED_AT
        ,FLOW_DOCUMENT
    from source
)

select * from renamed