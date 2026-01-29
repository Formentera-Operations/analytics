with source as (
    select * from {{ source('oda', 'APINVOICEDETAIL') }}
),

renamed as (
    select
        -- ids
        id,
        apinvoicedetailidentity as ap_invoice_detail_identity,
        accountid as account_id,
        afeid as afe_id,
        allocationparentid as allocation_parent_id,
        companyid as company_id,
        distributioncompanyid as distribution_company_id,
        expensedeckid as expense_deck_id,
        expensedeckrevisionid as expense_deck_revision_id,
        expensedecksetid as expense_deck_set_id,
        invoiceid as invoice_id,
        sourcewellallocationdeckrevisionid as source_well_allocation_deck_revision_id,
        vendorid as vendor_id,
        wellallocationdeckid as well_allocation_deck_id,
        wellid as well_id,

        -- numerics
        expensedeckinterest as expense_deck_interest,
        gross88thsvalue as gross_88ths_value,
        gross88thsvolume as gross_88ths_volume,
        netvalue as net_value,
        netvolume as net_volume,
        serial,

        -- strings
        description,

        -- booleans
        currencyfluctuationpassthrough as currency_fluctuation_pass_through,
        isallocationgenerated as is_allocation_generated,
        isallocationparent as is_allocation_parent,

        -- timestamps
        createdate as created_at,
        recordinsertdate as record_inserted_at,
        recordupdatedate as record_updated_at,
        transactiondate as transaction_date,
        updatedate as updated_at,

        -- metadata
        "_meta/op" as _meta_op,
        flow_published_at,
        flow_document

    from source
)

select * from renamed