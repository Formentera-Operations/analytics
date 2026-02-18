{{
    config(
        materialized='view',
        tags=['oda', 'staging', 'formentera']
    )
}}

{#
    Staging model for ODA Well / Cost Center master data

    Source: ODA_BATCH_ODA_WELL (9.4K rows, batch)
    Grain: One row per well / cost center (id)

    Notes:
    - Core well dimension — wells double as cost centers in Quorum
    - Integer booleans converted to true/false via coalesce(COL = 1, false)
    - No CREATEEVENTID/UPDATEEVENTID in source
    - FLOW_DOCUMENT excluded (large JSON, not needed downstream)
#}

with

source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_WELL') }}
),

renamed as (
    select -- noqa: ST06
        -- identifiers
        ID::varchar as id,
        trim(CODE)::varchar as code,
        CODESORT::varchar as code_sort,
        trim(NAME)::varchar as name,
        trim(APINUMBER)::varchar as api_number,
        trim(PROPERTYREFERENCECODE)::varchar as property_reference_code,
        WELLIDENTITY::int as well_identity,
        NID::int as nid,

        -- location
        COUNTRYISOALPHA2CODE::varchar as country_iso_alpha2_code,
        COUNTRYISOALPHA3CODE::varchar as country_iso_alpha3_code,
        COUNTRYISONUMERICCODE::int as country_iso_numeric_code,
        trim(COUNTRYNAME)::varchar as country_name,
        STATECODE::varchar as state_code,
        trim(STATENAME)::varchar as state_name,
        trim(COUNTYNAME)::varchar as county_name,
        trim(LEGALDESCRIPTION)::varchar as legal_description,

        -- operating group and operator
        trim(OPERATINGGROUPCODE)::varchar as operating_group_code,
        trim(OPERATINGGROUPNAME)::varchar as operating_group_name,
        OPERATORID::varchar as operator_id,

        -- well classification
        trim(COSTCENTERTYPECODE)::varchar as cost_center_type_code,
        trim(COSTCENTERTYPENAME)::varchar as cost_center_type_name,
        coalesce(STRIPPERWELL = 1, false) as is_stripper_well,

        -- afe information
        AFEUSAGETYPE::int as afe_usage_type,
        AFEUSAGETYPEID::int as afe_usage_type_id,

        -- well status
        trim(WELLSTATUSTYPECODE)::varchar as well_status_type_code,
        trim(WELLSTATUSTYPENAME)::varchar as well_status_type_name,
        WELLSTATUSEFFECTIVEDATE::date as well_status_effective_date,
        trim(PRODUCTIONSTATUSNAME)::varchar as production_status_name,

        -- important dates
        SPUDDATE::date as spud_date,
        FIRSTPRODUCTIONDATE::date as first_production_date,
        SHUTINDATE::date as shut_in_date,
        INACTIVEDATE::date as inactive_date,

        -- billing and revenue flags
        coalesce(HOLDALLBILLING = 1, false) as is_hold_all_billing,
        trim(HOLDBILLINGCATEGORYNAME)::varchar as hold_billing_category_name,
        coalesce(SUSPENDALLREVENUE = 1, false) as is_suspend_all_revenue,
        trim(SUSPENDREVENUETYPENAME)::varchar as suspend_revenue_type_name,

        -- audit
        CREATEDATE::timestamp_ntz as created_at,
        UPDATEDATE::timestamp_ntz as updated_at,
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
        {{ dbt_utils.generate_surrogate_key(['id']) }} as wells_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        wells_sk,

        -- identifiers
        id,
        code,
        code_sort,
        name,
        api_number,
        property_reference_code,
        well_identity,
        nid,

        -- location
        country_iso_alpha2_code,
        country_iso_alpha3_code,
        country_iso_numeric_code,
        country_name,
        state_code,
        state_name,
        county_name,
        legal_description,

        -- operating group and operator
        operating_group_code,
        operating_group_name,
        operator_id,

        -- well classification
        cost_center_type_code,
        cost_center_type_name,
        is_stripper_well,

        -- afe information
        afe_usage_type,
        afe_usage_type_id,

        -- well status
        well_status_type_code,
        well_status_type_name,
        well_status_effective_date,
        production_status_name,

        -- important dates
        spud_date,
        first_production_date,
        shut_in_date,
        inactive_date,

        -- billing and revenue flags
        is_hold_all_billing,
        hold_billing_category_name,
        is_suspend_all_revenue,
        suspend_revenue_type_name,

        -- audit
        created_at,
        updated_at,
        record_inserted_at,
        record_updated_at,

        -- dbt metadata
        _loaded_at,

        -- ingestion metadata
        _flow_published_at

    from enhanced
)

select * from final
