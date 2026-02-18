{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA AR advance records.

    Source: ODA_BATCH_ODA_ARADVANCE (Estuary batch, 27 rows)
    Grain: One row per advance (id)

    Notes:
    - Batch table — no CDC soft-delete filtering
    - Links advances to AFEs, companies, wells, expense decks, and vouchers
    - AFEID references unsynchronized table — no relationships test
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARADVANCE') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        trim(id)::varchar as id,
        aradvanceidentity::int as ar_advance_identity,

        -- relationships
        trim(afeid)::varchar as afe_id,
        trim(companyid)::varchar as company_id,
        trim(currencyid)::varchar as currency_id,
        trim(expensedeckid)::varchar as expense_deck_id,
        trim(voucherid)::varchar as voucher_id,
        trim(wellid)::varchar as well_id,

        -- dates
        advancedate::date as advance_date,

        -- descriptive
        description,

        -- financial
        expensedeckinteresttotal::decimal(18, 2) as expense_deck_interest_total,
        grossamount::decimal(18, 2) as gross_amount,
        netamount::decimal(18, 2) as net_amount,

        -- flags
        isaftercasing::boolean as is_after_casing,
        posted::boolean as is_posted,

        -- audit
        createdate::timestamp_ntz as created_at,
        trim(createeventid)::varchar as create_event_id,
        updatedate::timestamp_ntz as updated_at,
        trim(updateeventid)::varchar as update_event_id,
        recordinsertdate::timestamp_ntz as record_inserted_at,
        recordupdatedate::timestamp_ntz as record_updated_at,

        -- ingestion metadata
        flow_published_at::timestamp_tz as _flow_published_at

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as aradvance_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        aradvance_sk,

        -- identifiers
        id,
        ar_advance_identity,

        -- relationships
        afe_id,
        company_id,
        currency_id,
        expense_deck_id,
        voucher_id,
        well_id,

        -- dates
        advance_date,

        -- descriptive
        description,

        -- financial
        expense_deck_interest_total,
        gross_amount,
        net_amount,

        -- flags
        is_after_casing,
        is_posted,

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
