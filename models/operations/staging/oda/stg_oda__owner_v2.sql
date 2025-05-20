with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_OWNER_V2') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Entity relationship
        ENTITYID as entity_id,
        OWNERV2IDENTITY as owner_v2_identity,
        NID as n_id,
        
        -- Basic status
        ACTIVE as active,
        
        -- Tax information
        EXEMPTSTATETAX as exempt_state_tax,
        FEDERALWITHHOLDING as federal_withholding,
        PRINT1099 as print_1099,
        SECONDTINNOTICESENT as second_tin_notice_sent,
        K1TYPEID as k1_type_id,
        
        -- AR and Revenue configuration
        ARCROSSCLEAR as ar_cross_clear,
        DEFAULTARCURRENCYID as default_ar_currency_id,
        PRINTSTATEMENT as print_statement,
        AREMAIL as ar_email,
        AREMAILPREFERENCES as ar_email_preferences,
        HASAREMAIL as has_ar_email,
        CREATEARFROMREVENUECREDITS as create_ar_from_revenue_credits,
        CREATEREVENUEFROMARCREDITS as create_revenue_from_ar_credits,
        
        -- JIB configuration
        JIBEMAIL as jib_email,
        JIBEMAILPREFERENCES as jib_email_preferences,
        HASJIBEMAIL as has_jib_email,
        JIBLINKRECIPIENT as jib_link_recipient,
        MINIMUMJIBINVOICE as minimum_jib_invoice,
        
        -- Revenue configuration
        REVENUEEMAIL as revenue_email,
        REVENUEEMAILPREFERENCES as revenue_email_preferences,
        HASREVENUEEMAIL as has_revenue_email,
        REVENUECHECKSTUBREFERENCE as revenue_check_stub_reference,
        MINIMUMREVENUECHECK as minimum_revenue_check,
        WORKINGINTERESTONLY as working_interest_only,
        PAYOUTHISTORY as payout_history,
        NETTINGRULEID as netting_rule_id,
        
        -- Suspense and holds
        HOLDBILLING as hold_billing,
        HOLDREVENUE as hold_revenue,
        DEFAULTBILLINGSUSPENSECATEGORYID as default_billing_suspense_category_id,
        DEFAULTREVENUESUSPENSECATEGORYID as default_revenue_suspense_category_id,
        
        -- CDEX configuration
        CDEXRECIPIENTID as cdex_recipient_id,
        
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