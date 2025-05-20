with source as (
    select * from {{ source('oda', 'JIB') }}
),

renamed as (
    select
        -- ids
        id,
        jibidentity as jib_identity,
        afeid as afe_id,
        apinvoiceid as ap_invoice_id,
        arinvoiceid as ar_invoice_id,
        accountid as account_id,
        companyid as company_id,
        ownerid as owner_id,
        voucherid as voucher_id,
        wellid as well_id,

        -- dates
        billeddate as billed_date,
        effectivedate as effective_date,
        expensedate as expense_date,
        invoicedate as invoice_date,

        -- strings
        accountsort as account_sort,
        afecode as afe_code,
        aftercasepoint as after_case_point,
        apinvoicecode as ap_invoice_code,
        arinvoicecode as ar_invoice_code,
        billingstatus as billing_status,
        companycode as company_code,
        companyname as company_name,
        description,
        entitycode as entity_code,
        entityname as entity_name,
        entitytype as entity_type,
        exchangeratecode as exchange_rate_code,
        expensedeckcode as expense_deck_code,
        mainaccount as main_account,
        ownercode as owner_code,
        ownername as owner_name,
        ownersuspense as owner_suspense,
        referenceentitycode as reference_entity_code,
        referenceentitytype as reference_entity_type,
        subaccount as sub_account,
        suspense,
        wellcode as well_code,
        wellname as well_name,
        wellsuspense as well_suspense,

        -- numerics
        expensedeckchangecode as expense_deck_change_code,
        expensedeckinterest as expense_deck_interest,
        grossvalue as gross_value,
        netvalue as net_value,
        vouchercode as voucher_code,

        -- timestamps
        createdate as created_at,
        recordinsertdate as record_inserted_at,
        recordupdatedate as record_updated_at,
        updatedate as updated_at,

        -- metadata
        _meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed