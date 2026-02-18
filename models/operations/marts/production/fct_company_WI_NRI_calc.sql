{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'facts']
) }}

{#
    Company (FP Operations Co. 200) Working and Net Revenue Interest
    
    Purpose: FP Company Mineral Interest and Lease NRI, Product Interest Variance
    Grain: Working & Product Interest Per Well, Lease NRI Calculation
    
    Use cases:
    - Review Well Deck Interest 
    
    Sources:
    - dim_company_NRI
    - dim_company_WI
#}

select
    nri.GAS_NRI,
    nri.OIL_NRI,
    nri.PLANT_PROD_NRI,
    coalesce(nri.CC_WELL_ID, wi.CC_WELL_ID) as cc_well_id,
    coalesce(nri.CC_ARIES_ID, wi.CC_ARIES_ID) as cc_aries_id,
    coalesce(nri.CC_PHDWIN_ID, wi.CC_PHDWIN_ID) as cc_phdwin_id,
    coalesce(nri.CC_CHOSEN_ID, wi.CC_CHOSEN_ID) as cc_chosen_id,
    coalesce(nri.COMPANY_CODE_NAME, wi.COMPANY_CODE_NAME) as company_code_name,
    coalesce(nri.WELL_CODE, wi.WELL_CODE) as well_code,
    coalesce(nri.EID, wi.EID) as eid,
    coalesce(nri.WELL_NAME, wi.WELL_NAME) as well_name,


    coalesce(nri.API_NUMBER, wi.API_NUMBER) as api_number,

    coalesce(nri.STATE_CODE, wi.STATE_CODE) as state_code,
    coalesce(nri.COUNTY_NAME, wi.COUNTY_NAME) as county_name,
    (coalesce(wi.WORKING_INTEREST, 0)) as working_interest,

    case
        when wi.WORKING_INTEREST is null then 75
        when (nri.GAS_NRI / wi.WORKING_INTEREST) * 100 > 85 then 85
        else (nri.GAS_NRI / wi.WORKING_INTEREST) * 100
    end as gas_lease_nri,

    case
        when wi.WORKING_INTEREST is null then 75
        when (nri.OIL_NRI / wi.WORKING_INTEREST) * 100 > 85 then 85
        else (nri.OIL_NRI / wi.WORKING_INTEREST) * 100
    end as oil_lease_nri,

    case
        when wi.WORKING_INTEREST is null then 75
        when (nri.PLANT_PROD_NRI / wi.WORKING_INTEREST) * 100 > 85 then 85
        else (nri.PLANT_PROD_NRI / wi.WORKING_INTEREST) * 100
    end as plant_prod_lease_nri,

    --(nri.GAS_NRI/wi.WORKING_INTEREST) * 100 as "GAS_LEASE_NRI",
    --(nri.OIL_NRI/wi.WORKING_INTEREST) * 100 as "OIL_LEASE_NRI",
    --(nri.PLANT_PROD_NRI/wi.WORKING_INTEREST) * 100 as "PLANT_PROD_LEASE_NRI",

    (
        coalesce(
            nri.GAS_NRI = nri.OIL_NRI
            and nri.OIL_NRI = nri.PLANT_PROD_NRI
            and nri.GAS_NRI = nri.PLANT_PROD_NRI,
            false
        )
    ) as nri_match,

    coalesce(greatest(nri.CREATED_AT, wi.CREATED_AT), nri.CREATED_AT, wi.CREATED_AT) as created_at,
    coalesce(greatest(nri.UPDATED_AT, wi.UPDATED_AT), nri.UPDATED_AT, wi.UPDATED_AT) as updated_at
from ( -- noqa: ST05
    select
        CC_WELL_ID,
        CC_ARIES_ID,
        CC_PHDWIN_ID,
        CC_CHOSEN_ID,
        WELL_CODE,
        EID,
        WELL_NAME,
        API_NUMBER,
        STATE_CODE,
        COUNTY_NAME,
        DECK_NAME,
        LATEST_EFFECTIVE_DATE,
        COMPANY_CODE_NAME,
        max(case
            when PRODUCT_NAME = 'GAS' then TOTAL_INTEREST
            when PRODUCT_NAME = 'All Products' then TOTAL_INTEREST
        end) as gas_nri,
        max(case
            when PRODUCT_NAME = 'OIL' then TOTAL_INTEREST
            when PRODUCT_NAME = 'All Products' then TOTAL_INTEREST
        end) as oil_nri,
        max(case
            when PRODUCT_NAME = 'PLANT PROD' then TOTAL_INTEREST
            when PRODUCT_NAME = 'All Products' then TOTAL_INTEREST
        end) as plant_prod_nri,
        max(case
            when PRODUCT_NAME = 'SWD' then TOTAL_INTEREST
            when PRODUCT_NAME = 'All Products' then TOTAL_INTEREST
        end) as swd_nri,
        max(CREATED_AT) as created_at,
        max(UPDATED_AT) as updated_at
    from {{ ref('dim_company_NRI') }}
    group by
        CC_WELL_ID,
        CC_ARIES_ID,
        CC_PHDWIN_ID,
        CC_CHOSEN_ID,
        WELL_CODE,
        EID,
        WELL_NAME,
        API_NUMBER,
        STATE_CODE,
        COUNTY_NAME,
        DECK_NAME,
        LATEST_EFFECTIVE_DATE,
        COMPANY_CODE_NAME
) nri
full outer join ( -- noqa: ST05
    select
        CC_WELL_ID,
        CC_ARIES_ID,
        CC_PHDWIN_ID,
        CC_CHOSEN_ID,
        WELL_CODE,
        EID,
        WELL_NAME,
        API_NUMBER,
        STATE_CODE,
        COUNTY_NAME,
        DECK_NAME,
        LATEST_EFFECTIVE_DATE,
        COMPANY_CODE_NAME,
        max(case
            when INTEREST_TYPE = 'WI' then TOTAL_INTEREST
        end) as working_interest,
        max(CREATED_AT) as created_at,
        max(UPDATED_AT) as updated_at
    from {{ ref('dim_company_WI') }}
    group by
        CC_WELL_ID,
        CC_ARIES_ID,
        CC_PHDWIN_ID,
        CC_CHOSEN_ID,
        WELL_CODE,
        EID,
        WELL_NAME,
        API_NUMBER,
        STATE_CODE,
        COUNTY_NAME,
        DECK_NAME,
        LATEST_EFFECTIVE_DATE,
        COMPANY_CODE_NAME
) wi
    on nri.EID = wi.EID
