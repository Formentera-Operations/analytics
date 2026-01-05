with source as (
    select * from {{ source('oda', 'JIBDETAIL') }}
),

renamed as (
    select
        -- ids
        id,
        jibdetailidentity as jib_detail_identity,
        accountid as account_id,
        afeid as afe_id,
        arinvoiceid as ar_invoice_id,
        billingexchangerateid as billing_exchange_rate_id,
        companyid as company_id,
        createeventid as create_event_id,
        currencyid as currency_id,
        detailid as detail_id,
        entitycompanyid as entity_company_id,
        entitypurchaserid as entity_purchaser_id,
        entityvendorid as entity_vendor_id,
        expensedeckrevisionid as expense_deck_revision_id,
        gainlossexchangerateid as gain_loss_exchange_rate_id,
        grosseventid as gross_event_id,
        memocompanyid as memo_company_id,
        ownerid as owner_id,
        pendingredistributionid as pending_redistribution_id,
        redistributionvoucherid as redistribution_voucher_id,
        referenceentitycompanyid as reference_entity_company_id,
        referenceentityownerid as reference_entity_owner_id,
        referenceentitypurchaserid as reference_entity_purchaser_id,
        referenceentityvendorid as reference_entity_vendor_id,
        updateeventid as update_event_id,
        voucherid as voucher_id,
        wellid as well_id,

        -- dates
        accrualdate as accrual_date,
        billeddate as billed_date,
        expensedate as expense_date,
        gainlosspostedthroughdate as gain_loss_posted_through_date,

        -- numerics
        billingstatusid as billing_status_id,
        entitytypeid as entity_type_id,
        expensedeckinterest as expense_deck_interest,
        grossvalue as gross_value,
        netvalue as net_value,

        -- strings
        description,
        reference,

        -- booleans
        currencyfluctuationpassthrough as currency_fluctuation_pass_through,
        includeinaccrualreport as include_in_accrual_report,
        isaftercasing as is_after_casing,
        jibinoriginalcurrency as jib_in_original_currency,
        memoalsocodemissing as memo_also_code_missing,
        subtotalbysubaccount as subtotal_by_sub_account,
        userpostingmethod as user_posting_method,

        -- timestamps
        createdate as created_at,
        recordinsertdate as record_inserted_at,
        recordupdatedate as record_updated_at,
        updatedate as updated_at,

        -- metadata
        "_meta/op" as _meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed