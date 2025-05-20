with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_AFE_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Code identifiers
        CODE as code,
        CODESORT as code_sort,
        NAME as name,
        FULLNAME as full_name,
        FULLDESCRIPTION as full_description,
        
        -- AFE type information
        AFETYPEID as afe_type_id,
        AFETYPECODE as afe_type_code,
        AFETYPELABEL as afe_type_label,
        AFETYPEFULLNAME as afe_type_full_name,
        AFE_V2IDENTITY as afe_v2_identity,
        
        -- Application type
        APPLICATIONTYPEID as application_type_id,
        APPLICATIONTYPECODE as application_type_code,
        APPLICATIONTYPENAME as application_type_name,
        
        -- Budget usage
        BUDGETUSAGETYPEID as budget_usage_type_id,
        BUDGETUSAGETYPECODE as budget_usage_type_code,
        BUDGETUSAGETYPENAME as budget_usage_type_name,
        
        -- Field information
        FIELDID as field_id,
        FIELDCODE as field_code,
        FIELDCODESORT as field_code_sort,
        FIELDNAME as field_name,
        FIELDDESCRIPTION as field_description,
        
        -- Company information
        DEFAULTCOMPANYCODE as default_company_code,
        DEFAULTCOMPANYNAME as default_company_name,
        DEFAULTEXPENSEDECKCODE as default_expense_deck_code,
        
        -- Operating group
        OPERATINGGROUPID as operating_group_id,
        OPERATINGGROUPCODE as operating_group_code,
        OPERATINGGROUPNAME as operating_group_name,
        
        -- Well information
        WELLID as well_id,
        WELLCODE as well_code,
        WELLNAME as well_name,
        
        -- Other fields
        ACCOUNTGROUPNAME as account_group_name,
        OPERATORREFERENCE as operator_reference,
        NID as n_id,
        
        -- Dates
        CLOSEDATE as close_date,
        COMPLETIONDATE as completion_date,
        
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