{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Vendor master data

    Source: ODA_BATCH_ODA_VENDOR_V2 (4K rows, batch)
    Grain: One row per vendor (id)

    Notes:
    - Vendors link to entities via entity_id
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - CHANGEDSINCEPRINTED kept as int (not a pure boolean)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_VENDOR_V2') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        ENTITYID::varchar as entity_id,
        VENDORV2IDENTITY::int as vendor_v2_identity,
        NID::int as nid,

        -- status and flags
        coalesce(ACTIVE = 1, false) as is_active,
        CHANGEDSINCEPRINTED::int as changed_since_printed,
        coalesce(HOLDAPCHECKS = 1, false) as is_hold_ap_checks,
        coalesce(PRINT1099 = 1, false) as is_print_1099,
        coalesce(SECONDTINNOTICESENT = 1, false) as is_second_tin_notice_sent,

        -- ap configuration
        APDUEDATECALCULATEDBASEDON::int as ap_due_date_calculated_based_on,
        APCHECKSTUBREFERENCE::int as ap_check_stub_reference,
        APPAYMENTTYPEID::varchar as ap_payment_type_id,
        coalesce(ONEINVOICEPERCHECK = 1, false) as is_one_invoice_per_check,
        coalesce(TAKEALLDISCOUNTS = 1, false) as is_take_all_discounts,
        trim(TERMS)::varchar as terms,

        -- default accounts and currencies
        DEFAULTAPCURRENCYID::varchar as default_ap_currency_id,
        DEFAULTEXPENSEACCOUNTID::varchar as default_expense_account_id,
        MINIMUMAPCHECK::float as minimum_ap_check,
        MINIMUMCURRENCYID::varchar as minimum_currency_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as vendor_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        vendor_v2_sk,

        -- identifiers
        id,
        entity_id,
        vendor_v2_identity,
        nid,

        -- status and flags
        is_active,
        changed_since_printed,
        is_hold_ap_checks,
        is_print_1099,
        is_second_tin_notice_sent,

        -- ap configuration
        ap_due_date_calculated_based_on,
        ap_check_stub_reference,
        ap_payment_type_id,
        is_one_invoice_per_check,
        is_take_all_discounts,
        terms,

        -- default accounts and currencies
        default_ap_currency_id,
        default_expense_account_id,
        minimum_ap_check,
        minimum_currency_id,

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
