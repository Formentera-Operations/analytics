{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Company configuration

    Source: ODA_BATCH_ODA_COMPANY_V2 (39 rows, batch)
    Grain: One row per company (id)

    Notes:
    - Master data dimension — company-level configuration and fiscal periods
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_COMPANY_V2') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        CODE::number as code,
        CODESORT::varchar as code_sort,
        trim(NAME)::varchar as name,
        trim(FULLNAME)::varchar as full_name,
        trim(COMMENT)::varchar as comment,
        COMPANYV2IDENTITY::int as company_v2_identity,

        -- tax information
        TAXID::varchar as tax_id,
        TAXIDTYPEID::varchar as tax_id_type_id,
        coalesce(ISPARTNERSHIP = 1, false) as is_partnership,
        K1TYPEID::varchar as k1_type_id,

        -- contact information
        MAINCONTACTID::varchar as main_contact_id,
        trim(URL)::varchar as url,

        -- fiscal / accounting configuration
        BASISID::varchar as basis_id,
        FISCALYEARENDMONTH::int as fiscal_year_end_month,
        CURRENTFISCALYEAR::int as current_fiscal_year,

        -- current periods (TIMESTAMP_LTZ in source — represents current accounting period)
        CURRENTAPMONTH::timestamp_ntz as current_ap_month,
        CURRENTARMONTH::timestamp_ntz as current_ar_month,
        CURRENTGLMONTH::timestamp_ntz as current_gl_month,
        CURRENTJIBMONTH::timestamp_ntz as current_jib_month,
        CURRENTREVENUEMONTH::timestamp_ntz as current_revenue_month,

        -- ar / ap configuration
        ARINVOICESEQUENCEID::varchar as ar_invoice_sequence_id,
        APINVOICESEQUENCEID::varchar as ap_invoice_sequence_id,
        coalesce(ARSTATEMENTPRINTPENDING = 1, false) as is_ar_statement_print_pending,
        coalesce(ACCEPTMEMOENTRIES = 1, false) as is_accept_memo_entries,
        ACHID::varchar as ach_id,
        coalesce(ISACHCORPORATION = 1, false) as is_ach_corporation,

        -- default settings
        DEFAULTACCRUALITEMSID::varchar as default_accrual_items_id,
        DEFAULTEXPENSEINTERESTTYPEID::varchar as default_expense_interest_type_id,
        DEFAULTEXPENSECUSTOMINTERESTTYPEID::varchar as default_expense_custom_interest_type_id,
        DEFAULTEXPENSEENTITLEMENTINTEREST::varchar as default_expense_entitlement_interest,
        DEFAULTREVENUEINTERESTTYPEID::varchar as default_revenue_interest_type_id,
        DEFAULTREVENUECUSTOMINTERESTTYPEID::varchar as default_revenue_custom_interest_type_id,
        DEFAULTREVENUEENTITLEMENTINTEREST::varchar as default_revenue_entitlement_interest,

        -- retention configuration
        APDETAILRETENTIONMONTHS::int as ap_detail_retention_months,
        ARDETAILRETENTIONMONTHS::int as ar_detail_retention_months,
        GLDETAILRETENTIONMONTHS::int as gl_detail_retention_months,
        GLBALANCERETENTIONDATE::date as gl_balance_retention_date,
        GLBALANCERETENTIONYEARS::int as gl_balance_retention_years,
        JIBDETAILRETENTIONYEARS::int as jib_detail_retention_years,
        REVENUEDETAILRETENTIONMONTHS::int as revenue_detail_retention_months,

        -- other fields
        trim(CDEXCODE)::varchar as cdex_code,
        NID::int as nid,
        INACTIVEDATE::date as inactive_date,
        SUMMARIZERCHUNKSIZE::int as summarizer_chunk_size,
        TENANTID::varchar as tenant_id,

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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as company_v2_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        company_v2_sk,

        -- identifiers
        id,
        code,
        code_sort,
        name,
        full_name,
        comment,
        company_v2_identity,

        -- tax information
        tax_id,
        tax_id_type_id,
        is_partnership,
        k1_type_id,

        -- contact information
        main_contact_id,
        url,

        -- fiscal / accounting configuration
        basis_id,
        fiscal_year_end_month,
        current_fiscal_year,

        -- current periods
        current_ap_month,
        current_ar_month,
        current_gl_month,
        current_jib_month,
        current_revenue_month,

        -- ar / ap configuration
        ar_invoice_sequence_id,
        ap_invoice_sequence_id,
        is_ar_statement_print_pending,
        is_accept_memo_entries,
        ach_id,
        is_ach_corporation,

        -- default settings
        default_accrual_items_id,
        default_expense_interest_type_id,
        default_expense_custom_interest_type_id,
        default_expense_entitlement_interest,
        default_revenue_interest_type_id,
        default_revenue_custom_interest_type_id,
        default_revenue_entitlement_interest,

        -- retention configuration
        ap_detail_retention_months,
        ar_detail_retention_months,
        gl_detail_retention_months,
        gl_balance_retention_date,
        gl_balance_retention_years,
        jib_detail_retention_years,
        revenue_detail_retention_months,

        -- other fields
        cdex_code,
        nid,
        inactive_date,
        summarizer_chunk_size,
        tenant_id,

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
