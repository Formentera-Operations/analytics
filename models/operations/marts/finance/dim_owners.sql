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

with owners as (
    select
        -- =================================================================
        -- Owner Identity
        -- =================================================================
        o.id as owner_id,
        o.entity_id,
        e.code as owner_code,
        e.name as owner_name,
        e.full_name as owner_full_name,

        -- =================================================================
        -- Tax & Legal
        -- =================================================================
        e.tax_id,
        e.name_1099,
        o.is_print_1099,

        -- =================================================================
        -- Status Flags
        -- =================================================================
        o.is_active,
        o.is_working_interest_only,
        o.is_hold_revenue,
        o.is_hold_billing,

        -- =================================================================
        -- Payment Settings
        -- =================================================================
        o.minimum_revenue_check,
        o.minimum_jib_invoice,

        -- =================================================================
        -- Classification
        -- =================================================================
        case
            when o.is_working_interest_only then 'Working Interest Only'
            else 'All Interest Types'
        end as interest_scope,

        case
            when o.is_hold_revenue then 'Held'
            when not o.is_active then 'Inactive'
            else 'Active'
        end as payment_status,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _refreshed_at

    from {{ ref('stg_oda__owner_v2') }} o
    inner join {{ ref('stg_oda__entity_v2') }} e
        on o.entity_id = e.id
)

select * from owners
