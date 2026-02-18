{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Authorization for Expenditure (AFE) master records.

    Source: ODA_BATCH_ODA_AFE_V2 (Estuary batch, 2.2K rows)
    Grain: One row per AFE (id)

    Notes:
    - Batch table — no CDC delete filtering needed
    - Has _meta/op column (Estuary puts it on all tables) but NOT used for filtering
    - Heavily denormalized with type/group names inline for reporting convenience
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
    - Added default_company_id FK that was missing from prior version
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_AFE_V2') }}
),

renamed as (
    select
        -- identifiers
        trim(ID)::varchar as id,
        AFE_V2IDENTITY::int as afe_v2_identity,
        NID::int as n_id,
        trim(CODE)::varchar as code,
        trim(CODESORT)::varchar as code_sort,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,
        trim(FULLDESCRIPTION)::varchar as full_description,

        -- afe type
        trim(AFETYPEID)::varchar as afe_type_id,
        trim(AFETYPECODE)::varchar as afe_type_code,
        trim(AFETYPELABEL)::varchar as afe_type_label,
        trim(AFETYPEFULLNAME)::varchar as afe_type_full_name,

        -- application type
        APPLICATIONTYPEID::int as application_type_id,
        trim(APPLICATIONTYPECODE)::varchar as application_type_code,
        trim(APPLICATIONTYPENAME)::varchar as application_type_name,

        -- budget usage
        BUDGETUSAGETYPEID::int as budget_usage_type_id,
        trim(BUDGETUSAGETYPECODE)::varchar as budget_usage_type_code,
        trim(BUDGETUSAGETYPENAME)::varchar as budget_usage_type_name,

        -- field
        trim(FIELDID)::varchar as field_id,
        trim(FIELDCODE)::varchar as field_code,
        trim(FIELDCODESORT)::varchar as field_code_sort,
        trim(FIELDNAME)::varchar as field_name,
        trim(FIELDDESCRIPTION)::varchar as field_description,

        -- company
        trim(DEFAULTCOMPANYID)::varchar as default_company_id,
        trim(DEFAULTCOMPANYCODE)::varchar as default_company_code,
        trim(DEFAULTCOMPANYNAME)::varchar as default_company_name,
        trim(DEFAULTEXPENSEDECKCODE)::varchar as default_expense_deck_code,

        -- operating group
        trim(OPERATINGGROUPID)::varchar as operating_group_id,
        trim(OPERATINGGROUPCODE)::varchar as operating_group_code,
        trim(OPERATINGGROUPNAME)::varchar as operating_group_name,

        -- well
        trim(WELLID)::varchar as well_id,
        trim(WELLCODE)::varchar as well_code,
        trim(WELLNAME)::varchar as well_name,

        -- descriptive fields
        trim(ACCOUNTGROUPNAME)::varchar as account_group_name,
        trim(OPERATORREFERENCE)::varchar as operator_reference,

        -- dates
        CLOSEDATE::timestamp_ntz as close_date,
        COMPLETIONDATE::timestamp_ntz as completion_date,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        trim(CREATEEVENTID)::varchar as create_event_id,
        UPDATEDATE::timestamp_ntz as updated_at,
        trim(UPDATEEVENTID)::varchar as update_event_id,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as afe_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        afe_v2_sk,

        -- identifiers
        id,
        afe_v2_identity,
        n_id,
        code,
        code_sort,
        name,
        full_name,
        full_description,

        -- afe type
        afe_type_id,
        afe_type_code,
        afe_type_label,
        afe_type_full_name,

        -- application type
        application_type_id,
        application_type_code,
        application_type_name,

        -- budget usage
        budget_usage_type_id,
        budget_usage_type_code,
        budget_usage_type_name,

        -- field
        field_id,
        field_code,
        field_code_sort,
        field_name,
        field_description,

        -- company
        default_company_id,
        default_company_code,
        default_company_name,
        default_expense_deck_code,

        -- operating group
        operating_group_id,
        operating_group_code,
        operating_group_name,

        -- well
        well_id,
        well_code,
        well_name,

        -- descriptive fields
        account_group_name,
        operator_reference,

        -- dates
        close_date,
        completion_date,

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
