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

with purchasers as (
    select
        -- =================================================================
        -- Purchaser Identity
        -- =================================================================
        p.id as purchaser_id,
        p.entity_id,
        e.code as purchaser_code,
        e.name as purchaser_name,
        e.full_name as purchaser_full_name,

        -- =================================================================
        -- Tax & Legal
        -- =================================================================
        e.tax_id,
        e.name_1099,

        -- =================================================================
        -- Status Flags
        -- =================================================================
        p.is_active,

        -- =================================================================
        -- Classification
        -- =================================================================
        case
            when not p.is_active then 'Inactive'
            else 'Active'
        end as purchaser_status,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _refreshed_at

    from {{ ref('stg_oda__purchaser_v2') }} p
    inner join {{ ref('stg_oda__entity_v2') }} e
        on p.entity_id = e.id
)

select * from purchasers
