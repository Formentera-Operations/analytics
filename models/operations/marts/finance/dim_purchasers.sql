{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Purchasers
    
    Purpose: Purchaser entities (oil/gas/NGL buyers) with revenue attributes
    Grain: One row per purchaser (purchaser_id)
    
    Use cases:
    - Revenue by purchaser reporting
    - Commodity buyer analysis
    - Purchaser revenue receipts
    
    Sources:
    - stg_oda__purchaser_v2
    - stg_oda__entity_v2
#}

WITH purchasers AS (
    SELECT
        -- =================================================================
        -- Purchaser Identity
        -- =================================================================
        p.id AS purchaser_id,
        p.entity_id,
        e.code AS purchaser_code,
        e.name AS purchaser_name,
        e.full_name AS purchaser_full_name,
        
        -- =================================================================
        -- Tax & Legal
        -- =================================================================
        e.tax_id,
        e.name_1099,
        
        -- =================================================================
        -- Status Flags
        -- =================================================================
        p.active AS is_active,
        
        -- =================================================================
        -- Classification
        -- =================================================================
        CASE 
            WHEN NOT p.active THEN 'Inactive'
            ELSE 'Active'
        END AS purchaser_status,
        
        -- =================================================================
        -- Metadata
        -- =================================================================
        CURRENT_TIMESTAMP() AS _refreshed_at

    FROM {{ ref('stg_oda__purchaser_v2') }} p
    INNER JOIN {{ ref('stg_oda__entity_v2') }} e
        ON p.entity_id = e.id
)

SELECT * FROM purchasers