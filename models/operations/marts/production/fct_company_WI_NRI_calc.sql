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

SELECT 
    COALESCE(nri.CC_WELL_ID, wi.CC_WELL_ID) AS CC_WELL_ID,
    COALESCE(nri.CC_ARIES_ID, wi.CC_ARIES_ID) AS CC_ARIES_ID,
    COALESCE(nri.CC_PHDWIN_ID, wi.CC_PHDWIN_ID) AS CC_PHDWIN_ID,
    COALESCE(nri.CC_CHOSEN_ID, wi.CC_CHOSEN_ID) AS CC_CHOSEN_ID,
    COALESCE(nri.COMPANY_CODE_NAME, wi.COMPANY_CODE_NAME) AS COMPANY_CODE_NAME,
    COALESCE(nri.WELL_CODE, wi.WELL_CODE) AS WELL_CODE,
    COALESCE(nri.EID, wi.EID) AS EID,
    COALESCE(nri.WELL_NAME, wi.WELL_NAME) AS WELL_NAME,
    COALESCE(nri.API_NUMBER, wi.API_NUMBER) AS API_NUMBER,
    COALESCE(nri.STATE_CODE, wi.STATE_CODE) AS STATE_CODE,
    COALESCE(nri.COUNTY_NAME, wi.COUNTY_NAME) AS COUNTY_NAME,

    
    (CASE 
        WHEN wi.WORKING_INTEREST IS NULL THEN 0 ELSE wi.WORKING_INTEREST
    END) AS "WORKING_INTEREST",

    nri.GAS_NRI,
    nri.OIL_NRI,
    nri.PLANT_PROD_NRI,

    CASE 
    WHEN wi.WORKING_INTEREST IS NULL THEN 75
    WHEN (nri.GAS_NRI / wi.WORKING_INTEREST) * 100 > 85 THEN 85
    ELSE (nri.GAS_NRI / wi.WORKING_INTEREST) * 100 
    END as "GAS_LEASE_NRI",

    CASE 
    WHEN wi.WORKING_INTEREST IS NULL THEN 75
    WHEN (nri.OIL_NRI / wi.WORKING_INTEREST) * 100 > 85 THEN 85
    ELSE (nri.OIL_NRI / wi.WORKING_INTEREST) * 100 
    END as "OIL_LEASE_NRI",

    CASE 
    WHEN wi.WORKING_INTEREST IS NULL THEN 75
    WHEN (nri.PLANT_PROD_NRI / wi.WORKING_INTEREST) * 100 > 85 THEN 85
    ELSE (nri.PLANT_PROD_NRI / wi.WORKING_INTEREST) * 100 
    END as "PLANT_PROD_LEASE_NRI",

    --(nri.GAS_NRI/wi.WORKING_INTEREST) * 100 as "GAS_LEASE_NRI",
    --(nri.OIL_NRI/wi.WORKING_INTEREST) * 100 as "OIL_LEASE_NRI",
    --(nri.PLANT_PROD_NRI/wi.WORKING_INTEREST) * 100 as "PLANT_PROD_LEASE_NRI",

    (CASE WHEN  nri.GAS_NRI = nri.OIL_NRI
            AND nri.OIL_NRI = nri.PLANT_PROD_NRI
            AND nri.GAS_NRI = nri.PLANT_PROD_NRI
            THEN TRUE
            ELSE FALSE
            END) AS "NRI_MATCH",
        
    COALESCE(GREATEST(nri.CREATE_DATE, wi.CREATE_DATE), nri.CREATE_DATE, wi.CREATE_DATE) AS CREATE_DATE,
    COALESCE(GREATEST(nri.UPDATE_DATE, wi.UPDATE_DATE), nri.UPDATE_DATE, wi.UPDATE_DATE) AS UPDATE_DATE
FROM (
    SELECT 
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
        MAX(CASE 
            WHEN PRODUCT_NAME = 'GAS' THEN TOTAL_INTEREST 
            WHEN PRODUCT_NAME = 'All Products' THEN TOTAL_INTEREST 
        END) AS GAS_NRI,
        MAX(CASE 
            WHEN PRODUCT_NAME = 'OIL' THEN TOTAL_INTEREST 
            WHEN PRODUCT_NAME = 'All Products' THEN TOTAL_INTEREST 
        END) AS OIL_NRI,
        MAX(CASE 
            WHEN PRODUCT_NAME = 'PLANT PROD' THEN TOTAL_INTEREST 
            WHEN PRODUCT_NAME = 'All Products' THEN TOTAL_INTEREST 
        END) AS PLANT_PROD_NRI,
        MAX(CASE 
            WHEN PRODUCT_NAME = 'SWD' THEN TOTAL_INTEREST 
            WHEN PRODUCT_NAME = 'All Products' THEN TOTAL_INTEREST 
        END) AS SWD_NRI,
        MAX(CREATE_DATE) AS CREATE_DATE,
        MAX(UPDATE_DATE) AS UPDATE_DATE
    FROM {{ ref('dim_company_NRI') }}
    GROUP BY 
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
FULL OUTER JOIN (
    SELECT 
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
        MAX(CASE 
            WHEN INTEREST_TYPE = 'WI' THEN TOTAL_INTEREST 
        END) AS WORKING_INTEREST,
        MAX(CREATE_DATE) AS CREATE_DATE,
        MAX(UPDATE_DATE) AS UPDATE_DATE
    FROM {{ ref('dim_company_WI') }}
    GROUP BY 
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
ON nri.EID = wi.EID