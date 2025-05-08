with source as (

    select
        id as company_id,
        archived as is_archived,
        createdat as raw_created_at,
        updatedat as raw_updated_at,
        _portable_extracted as extracted_at,
        parse_json(properties) as properties

    from {{ source('hubspot', 'companies') }}

),

flattened as (

    select
        -- Primary Key and metadata
        coalesce(company_id, -1) as company_id,  -- Using -1 as a default for the primary key
        coalesce(is_archived, false) as is_archived,
        coalesce(extracted_at, '1900-01-01'::timestamp_ntz) as extracted_at,
        coalesce(raw_created_at, '1900-01-01'::timestamp_ntz) as raw_created_at,
        coalesce(raw_updated_at, '1900-01-01'::timestamp_ntz) as raw_updated_at,

        -- Core company properties
        -- Basic company information
        nullif(trim(properties:NAME::string), '') as name,
        nullif(trim(properties:DESCRIPTION::string), '') as description,
        nullif(trim(properties:INDUSTRY::string), '') as industry,
        nullif(trim(properties:STATUS::string), '') as status,
        nullif(trim(properties:LIFECYCLESTAGE::string), '') as lifecyclestage,
        
        -- Contact information
        nullif(trim(properties:EMAIL::string), '') as email,
        nullif(trim(properties:PHONE::string), '') as phone,
        nullif(trim(properties:FAX::string), '') as fax,
        
        -- Address information
        nullif(trim(properties:ADDRESS::string), '') as address,
        nullif(trim(properties:ADDRESS_LINE_ONE::string), '') as address_line_one,
        nullif(trim(properties:ADDRESS_LINE_TWO::string), '') as address_line_two,
        nullif(trim(properties:ADDRESS_LINE_THREE::string), '') as address_line_three,
        nullif(trim(properties:CITY::string), '') as city,
        nullif(trim(properties:STATE::string), '') as state,
        nullif(trim(properties:ZIP::string), '') as zip,
        nullif(trim(properties:COUNTRY::string), '') as country,
        
        -- Important dates (converted to proper timestamp type)
        coalesce(properties:CREATEDATE::timestamp_ntz, '1900-01-01'::timestamp_ntz) as created_at,
        try_to_timestamp(nullif(trim(properties:FIRST_CONTACT_CREATEDATE::string), '')) as first_contact_created_at,
        try_to_timestamp(nullif(trim(properties:HUBSPOT_OWNER_ASSIGNEDDATE::string), '')) as owner_assigned_at,
        try_to_timestamp(nullif(trim(properties:HS_LASTMODIFIEDDATE::string), '')) as last_modified_at,
        try_to_timestamp(nullif(trim(properties:NOTES_LAST_UPDATED::string), '')) as notes_last_updated_at,
        try_to_timestamp(nullif(trim(properties:HS_ANALYTICS_FIRST_TIMESTAMP::string), '')) as first_seen_at,
        try_to_timestamp(nullif(trim(properties:HS_ANALYTICS_LAST_TIMESTAMP::string), '')) as last_seen_at,
        
        -- Analytics metrics (converted to proper numeric types)
        coalesce(try_to_number(nullif(trim(properties:HS_ANALYTICS_NUM_PAGE_VIEWS::string), '')), 0) as page_views,
        coalesce(try_to_number(nullif(trim(properties:HS_ANALYTICS_NUM_VISITS::string), '')), 0) as total_visits,
        coalesce(try_to_number(nullif(trim(properties:NUM_ASSOCIATED_CONTACTS::string), '')), 0) as number_of_contacts,
        coalesce(try_to_number(nullif(trim(properties:NUM_CONTACTED_NOTES::string), '')), 0) as number_of_contact_notes,
        coalesce(try_to_number(nullif(trim(properties:NUM_NOTES::string), '')), 0) as total_notes,
        coalesce(try_to_number(nullif(trim(properties:WELL_COUNT::string), '')), 0) as well_count,
        coalesce(try_to_number(nullif(trim(properties:WELLBORE_COUNT::string), '')), 0) as wellbore_count,
        coalesce(try_to_number(nullif(trim(properties:HS_NUM_OPEN_DEALS::string), '')), 0) as number_of_open_deals,
        coalesce(try_to_number(nullif(trim(properties:HS_NUM_CHILD_COMPANIES::string), '')), 0) as number_of_child_companies,
        coalesce(try_to_number(nullif(trim(properties:DAYS_TO_CLOSE::string), '')), 0) as days_to_close,
        coalesce(try_to_number(nullif(trim(properties:MINIMUM_CHECK_AMOUNT::string), ''), 10, 2), 0) as minimum_check_amount,
        
        -- Tax and financial information
        nullif(trim(properties:TAX_STATUS::string), '') as tax_status,
        coalesce(properties:TAX_EXEMPT::boolean, false) as tax_exempt,
        nullif(trim(properties:TAX_ID_NUMBER::string), '') as tax_id_number,
        nullif(trim(properties:ACQUIRED_OWNER_NUMBER::string), '') as acquired_owner_number,
        nullif(trim(properties:VENDOR_NUMBER::string), '') as vendor_number,
        nullif(trim(properties:VENDOR_TYPE::string), '') as vendor_type,
        nullif(trim(properties:REVENUE_PAYMENT_TYPE::string), '') as revenue_payment_type,
        
        -- Boolean flags with defaults
        coalesce(properties:ACTIVE::boolean, false) as is_active,
        coalesce(properties:WORKING_INTEREST_OWNER::boolean, false) as is_working_interest_owner,
        coalesce(properties:HOLD_REVENUE_CHECKS::boolean, false) as is_revenue_checks_on_hold,
        coalesce(properties:W9_RECEIVED::boolean, false) as is_w9_received,
        try_to_date(nullif(trim(properties:W9_SIGNED_DATE::string), '')) as w9_signed_date,
        nullif(trim(properties:W9_TYPE::string), '') as w9_type,
        
        -- Owner and team information
        nullif(trim(properties:HUBSPOT_OWNER_ID::string), '') as hubspot_owner_id,
        nullif(trim(properties:HUBSPOT_TEAM_ID::string), '') as hubspot_team_id,
        nullif(trim(properties:HS_CREATED_BY_USER_ID::string), '') as created_by_user_id,
        nullif(trim(properties:HS_UPDATED_BY_USER_ID::string), '') as updated_by_user_id,
        
        -- Analytics source tracking
        nullif(trim(properties:HS_ANALYTICS_SOURCE::string), '') as first_conversion_source,
        nullif(trim(properties:HS_ANALYTICS_LATEST_SOURCE::string), '') as latest_conversion_source,
        try_to_timestamp(nullif(trim(properties:HS_ANALYTICS_LATEST_SOURCE_TIMESTAMP::string), '')) as latest_source_timestamp,
        
        -- System IDs and metadata
        nullif(trim(properties:HS_OBJECT_ID::string), '') as hubspot_object_id,
        nullif(trim(properties:HS_MERGED_OBJECT_IDS::string), '') as merged_object_ids,
        coalesce(properties:HS_WAS_IMPORTED::boolean, false) as is_imported,
        
        -- Activity tracking and engagement metrics
        try_to_timestamp(nullif(trim(properties:HS_LAST_BOOKED_MEETING_DATE::string), '')) as last_booked_meeting_at,
        try_to_timestamp(nullif(trim(properties:HS_LAST_LOGGED_CALL_DATE::string), '')) as last_logged_call_at,
        try_to_timestamp(nullif(trim(properties:HS_LAST_LOGGED_OUTGOING_EMAIL_DATE::string), '')) as last_outgoing_email_at,
        try_to_timestamp(nullif(trim(properties:HS_LAST_OPEN_TASK_DATE::string), '')) as last_open_task_at,
        try_to_timestamp(nullif(trim(properties:HS_LAST_SALES_ACTIVITY_DATE::string), '')) as last_sales_activity_at,
        try_to_timestamp(nullif(trim(properties:NOTES_LAST_CONTACTED::string), '')) as last_contacted_at,
        try_to_timestamp(nullif(trim(properties:HS_DATE_ENTERED_LEAD::string), '')) as entered_lead_at,
        nullif(trim(properties:HS_LAST_SALES_ACTIVITY_TYPE::string), '') as last_sales_activity_type,
        coalesce(try_to_number(nullif(trim(properties:HS_TIME_IN_LEAD::string), '')), 0) as time_in_lead_minutes,
        coalesce(try_to_number(nullif(trim(properties:NUM_CONVERSION_EVENTS::string), '')), 0) as number_of_conversion_events,
        
        -- Pipeline and deal information
        nullif(trim(properties:HS_PIPELINE::string), '') as pipeline,
        coalesce(try_to_number(nullif(trim(properties:HS_TARGET_ACCOUNT_PROBABILITY::string), ''), 10, 2), 0) as target_account_probability,
        coalesce(try_to_number(nullif(trim(properties:HS_NUM_BLOCKERS::string), '')), 0) as number_of_blockers,
        coalesce(try_to_number(nullif(trim(properties:HS_NUM_CONTACTS_WITH_BUYING_ROLES::string), '')), 0) as contacts_with_buying_roles,
        coalesce(try_to_number(nullif(trim(properties:HS_NUM_DECISION_MAKERS::string), '')), 0) as number_of_decision_makers,
        
        -- Analytics and visit data
        try_to_timestamp(nullif(trim(properties:HS_ANALYTICS_FIRST_VISIT_TIMESTAMP::string), '')) as first_visit_at,
        try_to_timestamp(nullif(trim(properties:HS_ANALYTICS_LAST_VISIT_TIMESTAMP::string), '')) as last_visit_at,
        nullif(trim(properties:HS_ANALYTICS_SOURCE_DATA_1::string), '') as analytics_source_data_1,
        nullif(trim(properties:HS_ANALYTICS_SOURCE_DATA_2::string), '') as analytics_source_data_2,
        nullif(trim(properties:HS_ANNUAL_REVENUE_CURRENCY_CODE::string), '') as annual_revenue_currency_code,
        
        -- Team and access information
        nullif(trim(properties:HS_ALL_ACCESSIBLE_TEAM_IDS::string), '') as accessible_team_ids,
        nullif(trim(properties:HS_ALL_ASSIGNED_BUSINESS_UNIT_IDS::string), '') as assigned_business_unit_ids,
        nullif(trim(properties:HS_ALL_OWNER_IDS::string), '') as all_owner_ids,
        nullif(trim(properties:HS_ALL_TEAM_IDS::string), '') as all_team_ids,
        nullif(trim(properties:HS_USER_IDS_OF_ALL_NOTIFICATION_FOLLOWERS::string), '') as notification_follower_ids,
        nullif(trim(properties:HS_USER_IDS_OF_ALL_OWNERS::string), '') as all_owner_user_ids,
        
        -- Business specific fields
        nullif(trim(properties:DBA_1099_NAME::string), '') as dba_1099_name,
        nullif(trim(properties:ENERGY_LINK_UPLOAD::string), '') as energy_link_upload,
        nullif(trim(properties:FEDERAL_WITHHOLDING::string), '') as federal_withholding,
        nullif(trim(properties:STATE_WITHHOLDING::string), '') as state_withholding,
        nullif(trim(properties:MINIMUM_JIB::string), '') as minimum_jib,
        nullif(trim(properties:NETTING_CODE::string), '') as netting_code,
        nullif(trim(properties:NETTING_CODE_HELPER::string), '') as netting_code_helper,
        nullif(trim(properties:OWNER_NUMBER___OGSYS::string), '') as owner_number_ogsys,
        nullif(trim(properties:REVENUE_CHECK_STUB::string), '') as revenue_check_stub,
        nullif(trim(properties:STATEMENT_DELIVERY::string), '') as statement_delivery,
        
        -- Well and unit information
        nullif(trim(properties:UNIT::string), '') as unit,
        nullif(trim(properties:UNIT_TYPE::string), '') as unit_type,
        nullif(trim(properties:WELL::string), '') as well,
        nullif(trim(properties:WELL_NAME::string), '') as well_name,
        nullif(trim(properties:WELL_STATUS::string), '') as well_status,
        nullif(trim(properties:WELL_TYPE::string), '') as well_type,
        nullif(trim(properties:WELLBORE::string), '') as wellbore,
        coalesce(try_to_number(nullif(trim(properties:YEARS_HELD::string), '')), 0) as years_held,
        
        -- Withholding information
        nullif(trim(properties:WITHHOLDING_REASON::string), '') as withholding_reason,
        try_to_date(nullif(trim(properties:WITHHOLDING_START_DATE::string), '')) as withholding_start_date,
        try_to_date(nullif(trim(properties:WITHHOLDING_STOP_DATE::string), '')) as withholding_stop_date,
        nullif(trim(properties:WITHHOLDING_TYPE::string), '') as withholding_type,
        nullif(trim(properties:VENDOR_WITHHOLDING::string), '') as vendor_withholding

    from source

)

select * from flattened