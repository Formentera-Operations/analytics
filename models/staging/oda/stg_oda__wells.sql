with source as (

    select * from fo_raw_db."oda"."Well"

),

renamed as (

    select

        -- Identifiers
        "Id" as well_id,
        "ApiNumber" as api_number,
        "WellIdentity" as well_identity,
        "PropertyReferenceCode" as property_reference_code,
        "Code" as well_code,

        -- Status & dates
        "WellStatusTypeName" as well_status_type_name,
        "WellStatusTypeCode" as well_status_type_code,
        "WellStatusEffectiveDate" as well_status_effective_date,
        "ProductionStatusName" as production_status_name,
        "PlugDate" as plug_date,
        "CompletionDate" as completion_date,
        "SpudDate" as spud_date,
        "ShutInDate" as shut_in_date,
        "FirstProductionDate" as first_production_date,

        -- Revenue & billing
        "SuspendAllRevenue" as suspend_all_revenue,
        "SuspendRevenueTypeName" as suspend_revenue_type_name,
        "HoldAllBilling" as hold_all_billing,
        "HoldBillingCategoryName" as hold_billing_category_name,

        -- Financials
        "AfeUsageTypeId" as afe_usage_type_id,
        "AfeUsageType" as afe_usage_type,
        "CostCenterTypeName" as cost_center_type_name,
        "CostCenterTypeCode" as cost_center_type_code,
        "HighCostExpirationDate" as high_cost_expiration_date,

        -- Location
        "CountryName" as country_name,
        "CountryIsoAlpha2Code" as country_iso_alpha2_code,
        "CountryIsoAlpha3Code" as country_iso_alpha3_code,
        "CountryIsoNumericCode" as country_iso_numeric_code,
        "StateName" as state_name,
        "StateCode" as state_code,
        "CountyName" as county_name,
        "LegalDescription" as legal_description,

        -- Operational
        "Name" as well_name,
        "NId" as n_id,
        "OperatingGroupCode" as operating_group_code,
        "OperatingGroupName" as operating_group_name,
        "CodeSort" as code_sort,
        "StripperWell" as stripper_well,
        "TotalDepthDate" as total_depth_date,
        "OperatorId" as operator_id,

        -- Metadata
        "CreateDate" as create_date,
        "UpdateDate" as update_date,
        "RecordInsertDate" as record_insert_date,
        "RecordUpdateDate" as record_update_date,
        "InactiveDate" as inactive_date,

        -- Fivetran metadata
        "_fivetran_deleted" as is_deleted,
        "_fivetran_synced" as _fivetran_synced,

        -- Optional for snapshots (SCD Type 2)
        "_fivetran_synced" as dbt_valid_from

    from source

)

select * from renamed
