{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Owner master data

    Source: ODA_BATCH_ODA_OWNER_V2 (42K rows, batch)
    Grain: One row per owner (id)

    Notes:
    - Owners link to entities via entity_id
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - PAYOUTHISTORY kept as int (ambiguous — may not be boolean)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_OWNER_V2') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ENTITYID::varchar as entity_id,
        OWNERV2IDENTITY::int as owner_v2_identity,
        NID::int as nid,

        -- status
        coalesce(ACTIVE = 1, false) as is_active,

        -- tax information
        coalesce(EXEMPTSTATETAX = 1, false) as is_exempt_state_tax,
        coalesce(FEDERALWITHHOLDING = 1, false) as is_federal_withholding,
        coalesce(PRINT1099 = 1, false) as is_print_1099,
        coalesce(SECONDTINNOTICESENT = 1, false) as is_second_tin_notice_sent,
        K1TYPEID::varchar as k1_type_id,

        -- ar configuration
        coalesce(ARCROSSCLEAR = 1, false) as is_ar_cross_clear,
        DEFAULTARCURRENCYID::varchar as default_ar_currency_id,
        coalesce(PRINTSTATEMENT = 1, false) as is_print_statement,
        trim(AREMAIL)::varchar as ar_email,
        trim(AREMAILPREFERENCES)::varchar as ar_email_preferences,
        coalesce(HASAREMAIL = 1, false) as has_ar_email,
        coalesce(CREATEARFROMREVENUECREDITS = 1, false) as is_create_ar_from_revenue_credits,
        coalesce(CREATEREVENUEFROMARCREDITS = 1, false) as is_create_revenue_from_ar_credits,

        -- jib configuration
        trim(JIBEMAIL)::varchar as jib_email,
        trim(JIBEMAILPREFERENCES)::varchar as jib_email_preferences,
        coalesce(HASJIBEMAIL = 1, false) as has_jib_email,
        coalesce(JIBLINKRECIPIENT = 1, false) as is_jib_link_recipient,
        MINIMUMJIBINVOICE::float as minimum_jib_invoice,

        -- revenue configuration
        trim(REVENUEEMAIL)::varchar as revenue_email,
        trim(REVENUEEMAILPREFERENCES)::varchar as revenue_email_preferences,
        coalesce(HASREVENUEEMAIL = 1, false) as has_revenue_email,
        trim(REVENUECHECKSTUBREFERENCE)::varchar as revenue_check_stub_reference,
        MINIMUMREVENUECHECK::float as minimum_revenue_check,
        coalesce(WORKINGINTERESTONLY = 1, false) as is_working_interest_only,
        PAYOUTHISTORY::int as payout_history,
        NETTINGRULEID::varchar as netting_rule_id,

        -- suspense and holds
        coalesce(HOLDBILLING = 1, false) as is_hold_billing,
        coalesce(HOLDREVENUE = 1, false) as is_hold_revenue,
        DEFAULTBILLINGSUSPENSECATEGORYID::varchar as default_billing_suspense_category_id,
        DEFAULTREVENUESUSPENSECATEGORYID::varchar as default_revenue_suspense_category_id,

        -- cdex configuration
        CDEXRECIPIENTID::varchar as cdex_recipient_id,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        CREATEEVENTID::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        UPDATEEVENTID::varchar as update_event_id,
        RECORDINSERTDATE::timestamp_ntz as record_inserted_at,
        RECORDUPDATEDATE::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        FLOW_PUBLISHED_AT::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as owner_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        owner_v2_sk,

        -- identifiers
        id,
        entity_id,
        owner_v2_identity,
        nid,

        -- status
        is_active,

        -- tax information
        is_exempt_state_tax,
        is_federal_withholding,
        is_print_1099,
        is_second_tin_notice_sent,
        k1_type_id,

        -- ar configuration
        is_ar_cross_clear,
        default_ar_currency_id,
        is_print_statement,
        ar_email,
        ar_email_preferences,
        has_ar_email,
        is_create_ar_from_revenue_credits,
        is_create_revenue_from_ar_credits,

        -- jib configuration
        jib_email,
        jib_email_preferences,
        has_jib_email,
        is_jib_link_recipient,
        minimum_jib_invoice,

        -- revenue configuration
        revenue_email,
        revenue_email_preferences,
        has_revenue_email,
        revenue_check_stub_reference,
        minimum_revenue_check,
        is_working_interest_only,
        payout_history,
        netting_rule_id,

        -- suspense and holds
        is_hold_billing,
        is_hold_revenue,
        default_billing_suspense_category_id,
        default_revenue_suspense_category_id,

        -- cdex configuration
        cdex_recipient_id,

        -- audit
        created_at,
        create_event_id,
        updated_at,
        update_event_id,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
