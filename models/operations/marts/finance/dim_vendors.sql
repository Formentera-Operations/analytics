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

with vendors as (
    select
        -- =================================================================
        -- Vendor Identity
        -- =================================================================
        v.id as vendor_id,
        v.entity_id,
        e.code as vendor_code,
        e.name as vendor_name,
        e.full_name as vendor_full_name,

        -- =================================================================
        -- Tax & Legal
        -- =================================================================
        e.tax_id,
        e.name_1099,
        v.is_print_1099,

        -- =================================================================
        -- Status Flags
        -- =================================================================
        v.is_active,
        v.is_hold_ap_checks,

        -- =================================================================
        -- Payment Settings
        -- =================================================================
        v.terms as payment_terms,
        v.minimum_ap_check,

        -- =================================================================
        -- Classification
        -- =================================================================
        case
            when v.is_hold_ap_checks then 'Held'
            when not v.is_active then 'Inactive'
            else 'Active'
        end as payment_status,

        case
            when trim(v.terms) in ('0D', '0M') then 'Due Immediately'
            when trim(v.terms) in ('7D', '10D', '14D', '15D') then 'Net 15 or Less'
            when trim(v.terms) in ('20D', '25D', '30D', '1M') then 'Net 30'
            when trim(v.terms) in ('35D', '45D') then 'Net 45'
            when trim(v.terms) = '60D' then 'Net 60'
            when v.terms is null or trim(v.terms) = '' then 'No Terms'
            else 'Other'
        end as payment_terms_category,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _refreshed_at

    from {{ ref('stg_oda__vendor_v2') }} v
    inner join {{ ref('stg_oda__entity_v2') }} e
        on v.entity_id = e.id
)

select * from vendors
