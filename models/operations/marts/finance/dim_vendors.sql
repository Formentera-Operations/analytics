{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Vendors
    
    Purpose: Vendor entities with AP and payment attributes
    Grain: One row per vendor (vendor_id)
    
    Use cases:
    - Accounts payable reporting
    - Vendor spend analysis
    - 1099 reporting
    - AP aging
    
    Sources:
    - stg_oda__vendor_v2
    - stg_oda__entity_v2
#}

WITH vendors AS (
    SELECT
        -- =================================================================
        -- Vendor Identity
        -- =================================================================
        v.id AS vendor_id,
        v.entity_id,
        e.code AS vendor_code,
        e.name AS vendor_name,
        e.full_name AS vendor_full_name,
        
        -- =================================================================
        -- Tax & Legal
        -- =================================================================
        e.tax_id,
        e.name_1099,
        v.print_1099,
        
        -- =================================================================
        -- Status Flags
        -- =================================================================
        v.active AS is_active,
        v.hold_ap_checks AS is_hold_ap_checks,
        
        -- =================================================================
        -- Payment Settings
        -- =================================================================
        v.terms AS payment_terms,
        v.minimum_ap_check,
        
        -- =================================================================
        -- Classification
        -- =================================================================
        CASE 
            WHEN v.hold_ap_checks THEN 'Held'
            WHEN NOT v.active THEN 'Inactive'
            ELSE 'Active'
        END AS payment_status,
        
        CASE 
            WHEN TRIM(v.terms) IN ('0D', '0M') THEN 'Due Immediately'
            WHEN TRIM(v.terms) IN ('7D', '10D', '14D', '15D') THEN 'Net 15 or Less'
            WHEN TRIM(v.terms) IN ('20D', '25D', '30D', '1M') THEN 'Net 30'
            WHEN TRIM(v.terms) IN ('35D', '45D') THEN 'Net 45'
            WHEN TRIM(v.terms) = '60D' THEN 'Net 60'
            WHEN v.terms IS NULL OR TRIM(v.terms) = '' THEN 'No Terms'
            ELSE 'Other'
        END AS payment_terms_category,
        
        -- =================================================================
        -- Metadata
        -- =================================================================
        CURRENT_TIMESTAMP() AS _refreshed_at

    FROM {{ ref('stg_oda__vendor_v2') }} v
    INNER JOIN {{ ref('stg_oda__entity_v2') }} e
        ON v.entity_id = e.id
)

SELECT * FROM vendors