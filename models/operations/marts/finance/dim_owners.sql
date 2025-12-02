{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Owners
    
    Purpose: Owner entities with revenue distribution and 1099 attributes
    Grain: One row per owner (owner_id)
    
    Use cases:
    - Revenue distribution reporting
    - Owner statement generation
    - 1099 reporting
    - Working/royalty interest analysis
    
    Sources:
    - stg_oda__owner_v2
    - stg_oda__entity_v2
#}

WITH owners AS (
    SELECT
        -- =================================================================
        -- Owner Identity
        -- =================================================================
        o.id AS owner_id,
        o.entity_id,
        e.code AS owner_code,
        e.name AS owner_name,
        e.full_name AS owner_full_name,
        
        -- =================================================================
        -- Tax & Legal
        -- =================================================================
        e.tax_id,
        e.name_1099,
        o.print_1099,
        
        -- =================================================================
        -- Status Flags
        -- =================================================================
        o.active AS is_active,
        o.working_interest_only AS is_working_interest_only,
        o.hold_revenue AS is_hold_revenue,
        o.hold_billing AS is_hold_billing,
        
        -- =================================================================
        -- Payment Settings
        -- =================================================================
        o.minimum_revenue_check,
        o.minimum_jib_invoice,
        
        -- =================================================================
        -- Classification
        -- =================================================================
        CASE 
            WHEN o.working_interest_only THEN 'Working Interest Only'
            ELSE 'All Interest Types'
        END AS interest_scope,
        
        CASE 
            WHEN o.hold_revenue THEN 'Held'
            WHEN NOT o.active THEN 'Inactive'
            ELSE 'Active'
        END AS payment_status,
        
        -- =================================================================
        -- Metadata
        -- =================================================================
        CURRENT_TIMESTAMP() AS _refreshed_at

    FROM {{ ref('stg_oda__owner_v2') }} o
    INNER JOIN {{ ref('stg_oda__entity_v2') }} e
        ON o.entity_id = e.id
)

SELECT * FROM owners
