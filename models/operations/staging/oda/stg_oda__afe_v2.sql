with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_AFE_V2') }}

),

renamed as (

    select
        -- Primary Identifiers
        ID as id,
        AFE_V2IDENTITY as afe_v2_identity,  -- Unique identity for AFE v2
        NID as n_id,                        -- Numeric identifier
        
        -- Basic AFE Information
        CODE as code,                       -- AFE code/number
        CODESORT as code_sort,              -- AFE code for sorting purposes
        NAME as name,                       -- Short name/title of the AFE
        FULLNAME as full_name,              -- Complete name of the AFE
        FULLDESCRIPTION as full_description, -- Detailed description of the AFE purpose
        
        -- AFE Type Classification
        AFETYPEID as afe_type_id,           -- Foreign key to AFE type
        AFETYPECODE as afe_type_code,       -- Code representing the type of AFE
        AFETYPELABEL as afe_type_label,     -- Display label for the AFE type
        AFETYPEFULLNAME as afe_type_full_name, -- Full name of the AFE type
        
        -- Application Type Details
        APPLICATIONTYPEID as application_type_id,      -- Foreign key to application type
        APPLICATIONTYPECODE as application_type_code,  -- Code for the application type
        APPLICATIONTYPENAME as application_type_name,  -- Name of the application type
        
        -- Budget Usage Classification
        BUDGETUSAGETYPEID as budget_usage_type_id,    -- Foreign key to budget usage type
        BUDGETUSAGETYPECODE as budget_usage_type_code, -- Code for budget usage category
        BUDGETUSAGETYPENAME as budget_usage_type_name, -- Name of the budget usage type
        
        -- Field Information
        FIELDID as field_id,                -- Foreign key to field
        FIELDCODE as field_code,            -- Code identifying the field
        FIELDCODESORT as field_code_sort,   -- Field code for sorting purposes
        FIELDNAME as field_name,            -- Name of the field
        FIELDDESCRIPTION as field_description, -- Description of the field
        
        -- Company Details
        DEFAULTCOMPANYCODE as default_company_code,      -- Default company code for this AFE
        DEFAULTCOMPANYNAME as default_company_name,      -- Name of the default company
        DEFAULTEXPENSEDECKCODE as default_expense_deck_code, -- Default expense deck code
        
        -- Operating Group Information
        OPERATINGGROUPID as operating_group_id,      -- Foreign key to operating group
        OPERATINGGROUPCODE as operating_group_code,  -- Code for the operating group
        OPERATINGGROUPNAME as operating_group_name,  -- Name of the operating group
        
        -- Well Information
        WELLID as well_id,                 -- Foreign key to well
        WELLCODE as well_code,             -- Code identifying the well
        WELLNAME as well_name,             -- Name of the well
        
        -- Additional Attributes
        ACCOUNTGROUPNAME as account_group_name,    -- Name of the account group
        OPERATORREFERENCE as operator_reference,   -- Reference number from operator
        
        -- Important Dates
        CLOSEDATE as close_date,           -- Date when the AFE was closed
        COMPLETIONDATE as completion_date, -- Date when the AFE was completed
        
        -- Metadata and Audit Fields
        CREATEDATE as create_date,         -- Record creation timestamp
        CREATEEVENTID as create_event_id,  -- Event ID for creation
        UPDATEDATE as update_date,         -- Last update timestamp
        UPDATEEVENTID as update_event_id,  -- Event ID for last update
        RECORDINSERTDATE as record_insert_date,  -- Database insert timestamp
        RECORDUPDATEDATE as record_update_date,  -- Database update timestamp
        FLOW_PUBLISHED_AT as flow_published_at,  -- Data flow publication timestamp
        
        -- Raw JSON Document
        FLOW_DOCUMENT as flow_document      -- Complete JSON document for reference

    from source

)

select * from renamed