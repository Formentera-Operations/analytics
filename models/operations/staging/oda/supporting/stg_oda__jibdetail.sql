{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Joint Interest Billing (Detail)

    Source: ODA_JIBDETAIL (Estuary CDC)
    Grain: One row per JIB detail line item (id)

    Notes:
    - Soft deletes filtered out (operation_type = 'd')
    - Normalized detail view with FK references to entity/account/well tables
    - For denormalized summary with codes/names, see stg_oda__jib
    - No direct FK relationship between JIBDETAIL and JIB (parallel views)
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_JIBDETAIL') }}
),

renamed as (
    select
        -- identifiers
        ID::varchar as id,
        JIBDETAILIDENTITY::int as jib_detail_identity,
        DETAILID::varchar as detail_id,

        -- relationships
        ACCOUNTID::varchar as account_id,
        AFEID::varchar as afe_id,
        ARINVOICEID::varchar as ar_invoice_id,
        COMPANYID::varchar as company_id,
        OWNERID::varchar as owner_id,
        WELLID::varchar as well_id,
        VOUCHERID::varchar as voucher_id,
        EXPENSEDECKREVISIONID::varchar as expense_deck_revision_id,

        -- entity references
        ENTITYTYPEID::int as entity_type_id,
        ENTITYCOMPANYID::varchar as entity_company_id,
        ENTITYPURCHASERID::varchar as entity_purchaser_id,
        ENTITYVENDORID::varchar as entity_vendor_id,
        REFERENCEENTITYCOMPANYID::varchar as reference_entity_company_id,
        REFERENCEENTITYOWNERID::varchar as reference_entity_owner_id,
        REFERENCEENTITYPURCHASERID::varchar as reference_entity_purchaser_id,
        REFERENCEENTITYVENDORID::varchar as reference_entity_vendor_id,
        MEMOCOMPANYID::varchar as memo_company_id,

        -- dates
        ACCRUALDATE::date as accrual_date,
        BILLEDDATE::date as billed_date,
        EXPENSEDATE::date as expense_date,
        GAINLOSSPOSTEDTHROUGHDATE::date as gain_loss_posted_through_date,

        -- financial
        BILLINGSTATUSID::int as billing_status_id,
        EXPENSEDECKINTEREST::float as expense_deck_interest,
        GROSSVALUE::float as gross_value,
        NETVALUE::float as net_value,

        -- currency
        CURRENCYID::varchar as currency_id,
        BILLINGEXCHANGERATEID::varchar as billing_exchange_rate_id,
        GAINLOSSEXCHANGERATEID::varchar as gain_loss_exchange_rate_id,
        BILLINGSUSPENSECATEGORYID::varchar as billing_suspense_category_id,
        PENDINGREDISTRIBUTIONID::varchar as pending_redistribution_id,

        -- flags
        REDISTRIBUTIONVOUCHERID::varchar as redistribution_voucher_id,
        trim(DESCRIPTION)::varchar as description,
        trim(REFERENCE)::varchar as reference,
        CREATEEVENTID::varchar as create_event_id,
        GROSSEVENTID::varchar as gross_event_id,

        -- suspense
        UPDATEEVENTID::varchar as update_event_id,

        -- redistribution
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,

        -- descriptive
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- event tracking
        "_meta/op"::varchar as _operation_type,
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at,
        coalesce(CURRENCYFLUCTUATIONPASSTHROUGH = 1, false) as is_currency_fluctuation_passthrough,

        -- audit
        coalesce(JIBINORIGINALCURRENCY = 1, false) as is_jib_in_original_currency,
        coalesce(INCLUDEINACCRUALREPORT = 1, false) as is_include_in_accrual_report,
        coalesce(ISAFTERCASING = 1, false) as is_after_casing,
        coalesce(MEMOALSOCODEMISSING = 1, false) as is_memo_also_code_missing,

        -- ingestion metadata
        coalesce(SUBTOTALBYSUBACCOUNT = 1, false) as is_subtotal_by_sub_account,
        coalesce(USERPOSTINGMETHOD = 1, false) as is_user_posting_method

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as jibdetail_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        jibdetail_sk,

        -- identifiers
        id,
        jib_detail_identity,
        detail_id,

        -- relationships
        account_id,
        afe_id,
        ar_invoice_id,
        company_id,
        owner_id,
        well_id,
        voucher_id,
        expense_deck_revision_id,

        -- entity references
        entity_type_id,
        entity_company_id,
        entity_purchaser_id,
        entity_vendor_id,
        reference_entity_company_id,
        reference_entity_owner_id,
        reference_entity_purchaser_id,
        reference_entity_vendor_id,
        memo_company_id,

        -- dates
        accrual_date,
        billed_date,
        expense_date,
        gain_loss_posted_through_date,

        -- financial
        billing_status_id,
        expense_deck_interest,
        gross_value,
        net_value,

        -- currency
        currency_id,
        billing_exchange_rate_id,
        gain_loss_exchange_rate_id,
        is_currency_fluctuation_passthrough,
        is_jib_in_original_currency,

        -- flags
        is_include_in_accrual_report,
        is_after_casing,
        is_memo_also_code_missing,
        is_subtotal_by_sub_account,
        is_user_posting_method,

        -- suspense
        billing_suspense_category_id,

        -- redistribution
        pending_redistribution_id,
        redistribution_voucher_id,

        -- descriptive
        description,
        reference,

        -- event tracking
        create_event_id,
        gross_event_id,
        update_event_id,

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
