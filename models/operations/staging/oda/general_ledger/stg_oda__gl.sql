{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA General Ledger

    Source: GL (Estuary CDC, 180M rows)
    Grain: One row per GL entry (id)

    Notes:
    - Soft deletes filtered out (_operation_type = 'd')
    - Currency defaults to USD when null (tracked via is_currency_defaulted)
    - Financial values default to 0 when null
    - No surrogate key -- view over 180M rows; MD5 at query time is unacceptable
    - Downstream joins use natural key id (aliased as gl_id where needed)
#}

with

source as (

    select * from {{ source('oda', 'GL') }}

),

renamed as (  -- noqa: ST06

    select
        -- identifiers
        ID::varchar as id,
        COMPANYID::varchar as company_id,
        ACCOUNTID::varchar as account_id,
        JOURNALDATEKEY::varchar as journal_date_key,
        GLIDENTITY::int as gl_identity,
        NID::int as n_id,
        AFEID::varchar as afe_id,
        ALLOCATIONPARENTID::varchar as allocation_parent_id,
        APCHECKID::varchar as ap_check_id,
        APINVOICEID::varchar as ap_invoice_id,
        ARINVOICEID::varchar as ar_invoice_id,
        CHECKREVENUEID::varchar as check_revenue_id,
        ENTITYCOMPANYID::varchar as entity_company_id,
        ENTITYID::varchar as entity_id,
        ENTITYOWNERID::varchar as entity_owner_id,
        ENTITYPURCHASERID::varchar as entity_purchaser_id,
        ENTITYVENDORID::varchar as entity_vendor_id,
        EXCHANGERATEID::varchar as exchange_rate_id,
        LOCATIONCOMPANYID::varchar as location_company_id,
        LOCATIONOWNERID::varchar as location_owner_id,
        LOCATIONPURCHASERID::varchar as location_purchaser_id,
        LOCATIONVENDORID::varchar as location_vendor_id,
        LOCATIONWELLID::varchar as location_well_id,
        PENDINGEXPENSEDECKID::varchar as pending_expense_deck_id,
        PENDINGEXPENSEDECKSETID::varchar as pending_expense_deck_set_id,
        PURCHASERREVENUERECEIPTID::varchar as purchaser_revenue_receipt_id,
        SOURCEEXPENSEDECKREVISIONID::varchar as source_expense_deck_revision_id,
        SOURCEREVENUEDECKREVISIONID::varchar as source_revenue_deck_revision_id,
        SOURCEWELLALLOCATIONDECKID::varchar as source_well_allocation_deck_id,
        SOURCEWELLALLOCATIONDECKREVISIONID::varchar as source_well_allocation_deck_revision_id,
        VOUCHERID::varchar as voucher_id,
        WELLID::varchar as well_id,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEEVENTID::varchar as update_event_id,

        -- dates
        ACCRUALDATEKEY::float as accrual_date_key,
        CASHDATEKEY::float as cash_date_key,
        trim(DESCRIPTION)::varchar as description,
        trim(SOURCEMODULE)::varchar as source_module,
        trim(SOURCEMODULECODE)::varchar as source_module_code,

        -- descriptive
        SOURCEMODULEID::int as source_module_id,
        trim(SOURCEMODULENAME)::varchar as source_module_name,
        trim(LOCATIONTYPE)::varchar as location_type,
        trim(REFERENCE)::varchar as reference,
        trim(RECONCILEDTYPECODE)::varchar as reconciled_type_code,
        RECONCILIATIONTYPEID::int as reconciliation_type_id,
        trim(PAYMENTTYPECODE)::varchar as payment_type_code,
        PAYMENTTYPEID::int as payment_type_id,
        ENTRYGROUP::int as entry_group,
        ORDINAL::int as ordinal,
        FLUCTUATIONTYPEID::int as fluctuation_type_id,
        MANUALENTRYREFERENCETYPEID::int as manual_entry_reference_type_id,
        CONVERTCURRENCYTYPEID::int as convert_currency_type_id,
        trim(CURRENCYID)::varchar as currency_id,
        coalesce(GROSSVALUE, 0)::decimal(19, 4) as gross_value,
        coalesce(GROSSVOLUME, 0)::decimal(19, 4) as gross_volume,
        coalesce(NETVALUE, 0)::decimal(19, 4) as net_value,

        -- financial
        coalesce(NETVOLUME, 0)::decimal(19, 4) as net_volume,
        POSTED::boolean as is_posted,
        GENERATEDENTRY::boolean as is_generated_entry,
        CONVERTCURRENCY::boolean as is_convert_currency,

        -- flags
        INCLUDEINACCRUALREPORT::boolean as is_include_in_accrual_report,
        INCLUDEINCASHREPORT::boolean as is_include_in_cash_report,
        INCLUDEINJOURNALREPORT::boolean as is_include_in_journal_report,
        ISALLOCATIONGENERATED::boolean as is_allocation_generated,
        ISALLOCATIONPARENT::boolean as is_allocation_parent,
        PRESENTINACCRUALBALANCE::boolean as is_present_in_accrual_balance,
        PRESENTINCASHBALANCE::boolean as is_present_in_cash_balance,
        PRESENTINJOURNALBALANCE::boolean as is_present_in_journal_balance,
        RECONCILED::boolean as is_reconciled,
        RECONCILEDTRIAL::boolean as is_reconciled_trial,
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,

        -- audit
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,
        "_meta/op" as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_ntz as _flow_published_at,
        date(convert_timezone('UTC', JOURNALDATE)) as journal_date,

        -- ingestion metadata
        date(convert_timezone('UTC', ACCRUALDATE)) as accrual_date,
        date(convert_timezone('UTC', CASHDATE)) as cash_date

        -- FLOW_DOCUMENT excluded (large VARIANT column, unnecessary at staging layer)

    from source

),

filtered as (

    select * from renamed
    where
        _operation_type != 'd'
        and id is not null

),

enhanced as (

    select
        *,
        -- Business default: use 'USD' when currency is null
        coalesce(currency_id is null, false) as is_currency_defaulted,
        -- Description normalization
        case
            when trim(description) in ('.', '') or description is null then 'No Description'
            else description
        end as description_normalized,
        current_timestamp() as _loaded_at
    from filtered

),

final as (

    select
        -- identifiers
        id,
        company_id,
        account_id,
        journal_date_key,
        gl_identity,
        n_id,
        afe_id,
        allocation_parent_id,
        ap_check_id,
        ap_invoice_id,
        ar_invoice_id,
        check_revenue_id,
        entity_company_id,
        entity_id,
        entity_owner_id,
        entity_purchaser_id,
        entity_vendor_id,
        exchange_rate_id,
        location_company_id,
        location_owner_id,
        location_purchaser_id,
        location_vendor_id,
        location_well_id,
        pending_expense_deck_id,
        pending_expense_deck_set_id,
        purchaser_revenue_receipt_id,
        source_expense_deck_revision_id,
        source_revenue_deck_revision_id,
        source_well_allocation_deck_id,
        source_well_allocation_deck_revision_id,
        voucher_id,
        well_id,
        create_event_id,
        update_event_id,

        -- dates
        journal_date,
        accrual_date_key,
        accrual_date,
        cash_date_key,
        cash_date,

        -- descriptive
        description,
        description_normalized,
        source_module,
        source_module_code,
        source_module_id,
        source_module_name,
        location_type,
        reference,
        reconciled_type_code,
        reconciliation_type_id,
        payment_type_code,
        payment_type_id,
        entry_group,
        ordinal,
        fluctuation_type_id,
        manual_entry_reference_type_id,
        convert_currency_type_id,
        currency_id,

        -- financial
        gross_value,
        gross_volume,
        net_value,
        net_volume,

        -- flags
        is_posted,
        is_generated_entry,
        is_convert_currency,
        is_currency_defaulted,
        is_include_in_accrual_report,
        is_include_in_cash_report,
        is_include_in_journal_report,
        is_allocation_generated,
        is_allocation_parent,
        is_present_in_accrual_balance,
        is_present_in_cash_balance,
        is_present_in_journal_balance,
        is_reconciled,
        is_reconciled_trial,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- ingestion metadata
        _operation_type,
        _flow_published_at,
        _loaded_at

    from enhanced

)

select * from final
