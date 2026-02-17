{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Accounts Payable Invoice Detail Lines

    Source: ODA_APINVOICEDETAIL (Estuary CDC)
    Grain: One row per AP invoice line item (id)

    Notes:
    - Soft deletes filtered out (operation_type = 'd')
    - Financial values cast to float
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FK: invoice_id references stg_oda__apinvoice.id
#}

with

source as (
    select * from {{ source('oda', 'ODA_APINVOICEDETAIL') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        APINVOICEDETAILIDENTITY::int as ap_invoice_detail_identity,
        INVOICEID::varchar as invoice_id,
        COMPANYID::varchar as company_id,
        VENDORID::varchar as vendor_id,
        ACCOUNTID::varchar as account_id,
        DISTRIBUTIONCOMPANYID::varchar as distribution_company_id,
        WELLID::varchar as well_id,
        AFEID::varchar as afe_id,
        SERIAL::int as serial,

        -- expense deck
        EXPENSEDECKID::varchar as expense_deck_id,
        EXPENSEDECKINTEREST::float as expense_deck_interest,
        EXPENSEDECKREVISIONID::varchar as expense_deck_revision_id,
        EXPENSEDECKSETID::varchar as expense_deck_set_id,

        -- financial
        GROSS88THSVALUE::float as gross_88ths_value,
        GROSS88THSVOLUME::float as gross_88ths_volume,
        NETVALUE::float as net_value,
        NETVOLUME::float as net_volume,
        trim(DESCRIPTION)::varchar as description,

        -- allocation
        ALLOCATIONPARENTID::varchar as allocation_parent_id,
        SOURCEWELLALLOCATIONDECKREVISIONID::varchar as source_well_allocation_deck_revision_id,
        WELLALLOCATIONDECKID::varchar as well_allocation_deck_id,
        TRANSACTIONDATE::date as transaction_date,
        CREATEDATE::timestamp_ntz as created_at,

        -- flags
        UPDATEDATE::timestamp_ntz as updated_at,

        -- dates
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,

        -- audit
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,
        "_meta/op"::varchar as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at,
        coalesce(ISALLOCATIONGENERATED = 1, false) as is_allocation_generated,

        -- ingestion metadata
        coalesce(ISALLOCATIONPARENT = 1, false) as is_allocation_parent,
        coalesce(CURRENCYFLUCTUATIONPASSTHROUGH = 1, false) as is_currency_fluctuation_passthrough

    from source
),

filtered as (
    select *
    from renamed
    where
        _operation_type != 'd'
        and id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as apinvoicedetail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        apinvoicedetail_sk,

        -- identifiers
        id,
        ap_invoice_detail_identity,
        invoice_id,
        company_id,
        vendor_id,
        account_id,
        distribution_company_id,
        well_id,
        afe_id,
        serial,

        -- expense deck
        expense_deck_id,
        expense_deck_interest,
        expense_deck_revision_id,
        expense_deck_set_id,

        -- financial
        gross_88ths_value,
        gross_88ths_volume,
        net_value,
        net_volume,
        description,

        -- allocation
        allocation_parent_id,
        is_allocation_generated,
        is_allocation_parent,
        source_well_allocation_deck_revision_id,
        well_allocation_deck_id,

        -- flags
        is_currency_fluctuation_passthrough,

        -- dates
        transaction_date,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _operation_type,
        _flow_published_at

    from enhanced
)

select * from final
