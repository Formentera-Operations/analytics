with source as (
    
    select * from {{ source('oda', 'ODA_BATCH_ODA_WELL') }}

),

renamed as (

    select
        -- Primary key
        ID as id,
        
        -- Well identifiers
        CODE as code,
        CODESORT as code_sort,
        NAME as name,
        APINUMBER as api_number,
        PROPERTYREFERENCECODE as property_reference_code,
        WELLIDENTITY as well_identity,
        NID as n_id,
        
        -- Location information
        COUNTRYISOALPHA2CODE as country_iso_alpha2_code,
        COUNTRYISOALPHA3CODE as country_iso_alpha3_code,
        COUNTRYISONUMERICCODE as country_iso_numeric_code,
        COUNTRYNAME as country_name,
        STATECODE as state_code,
        STATENAME as state_name,
        COUNTYNAME as county_name,
        LEGALDESCRIPTION as legal_description,
        
        -- Operating group and operator
        OPERATINGGROUPCODE as operating_group_code,
        OPERATINGGROUPNAME as operating_group_name,
        OPERATORID as operator_id,
        
        -- Well classification
        COSTCENTERTYPECODE as cost_center_type_code,
        COSTCENTERTYPENAME as cost_center_type_name,
        STRIPPERWELL as stripper_well,
        
        -- AFE information
        AFEUSAGETYPE as afe_usage_type,
        AFEUSAGETYPEID as afe_usage_type_id,
        
        -- Well status
        WELLSTATUSTYPECODE as well_status_type_code,
        WELLSTATUSTYPENAME as well_status_type_name,
        WELLSTATUSEFFECTIVEDATE as well_status_effective_date,
        PRODUCTIONSTATUSNAME as production_status_name,
        
        -- Important dates
        SPUDDATE as spud_date,
        FIRSTPRODUCTIONDATE as first_production_date,
        SHUTINDATE as shut_in_date,
        INACTIVEDATE as inactive_date,
        
        -- Billing and revenue flags
        HOLDALLBILLING as hold_all_billing,
        HOLDBILLINGCATEGORYNAME as hold_billing_category_name,
        SUSPENDALLREVENUE as suspend_all_revenue,
        SUSPENDREVENUETYPENAME as suspend_revenue_type_name,
        
        -- Metadata and timestamps
        CREATEDATE as create_date,
        UPDATEDATE as update_date,
        RECORDINSERTDATE as record_insert_date,
        RECORDUPDATEDATE as record_update_date,
        FLOW_PUBLISHED_AT as flow_published_at,
        
        -- Full document JSON for reference
        FLOW_DOCUMENT as flow_document

    from source

)

select * from renamed
