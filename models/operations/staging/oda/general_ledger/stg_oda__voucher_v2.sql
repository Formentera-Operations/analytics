{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Vouchers (V2)

    Source: ODA_VOUCHER_V2 (370K rows, batch)
    Grain: One row per voucher (id)

    Notes:
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_VOUCHER_V2') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        VOUCHERIDENTITY::int as voucher_identity,
        VOUCHERTYPEID::int as voucher_type_id,
        ORIGINATINGCOMPANYID::varchar as originating_company_id,
        CURRENCYID::varchar as currency_id,
        CONVERTCURRENCYTYPEID::varchar as convert_currency_type_id,
        IMPORTDATAID::varchar as import_data_id,

        -- descriptors
        trim(CODE)::varchar as code,
        trim(DESCRIPTION)::varchar as description,

        -- dates
        VOUCHERDATE::date as voucher_date,
        ACCRUALREVERSALDATE::date as accrual_reversal_date,
        DATEKEY::int as date_key,
        POSTEDDATE::date as posted_date,

        -- financial
        SUBLEDGERTOTAL::float as subledger_total,

        -- posted by
        POSTEDBYID::varchar as posted_by_id,
        POSTEDBYUSERID::varchar as posted_by_user_id,
        trim(POSTEDBYNAME)::varchar as posted_by_name,

        -- flags
        coalesce(DELETED = 1, false) as is_deleted,
        coalesce(HISTORICAL = 1, false) as is_historical,
        coalesce(POSTED = 1, false) as is_posted,
        coalesce(READYTOPOST = 1, false) as is_ready_to_post,
        coalesce(ISMANUALJOURNALACCRUAL = 1, false) as is_manual_journal_accrual,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
        CREATEEVENTID::varchar as create_event_id,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as voucher_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        voucher_v2_sk,

        -- identifiers
        id,
        voucher_identity,
        voucher_type_id,
        originating_company_id,
        currency_id,
        convert_currency_type_id,
        import_data_id,

        -- descriptors
        code,
        description,

        -- dates
        voucher_date,
        accrual_reversal_date,
        date_key,
        posted_date,

        -- financial
        subledger_total,

        -- posted by
        posted_by_id,
        posted_by_user_id,
        posted_by_name,

        -- flags
        is_deleted,
        is_historical,
        is_posted,
        is_ready_to_post,
        is_manual_journal_accrual,

        -- audit
        created_at,
        updated_at,
        create_event_id,
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
