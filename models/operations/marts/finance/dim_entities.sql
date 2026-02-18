{{ config(materialized='table') }}

with entities_base as (
    select
        e.id as entity_id,
        e.code as entity_code,
        e.name as entity_name,
        e.full_name as entity_full_name,
        e.tax_id,
        e.name_1099,

        -- Owner information (if entity is an owner)
        o.is_active as is_active_owner,
        o.is_working_interest_only as working_interest_only,
        o.is_print_1099 as owner_print_1099,
        o.is_hold_revenue as hold_revenue,
        o.is_hold_billing as owner_hold_billing,
        o.minimum_revenue_check,

        -- Vendor information (if entity is a vendor)
        v.is_active as is_active_vendor,
        v.is_print_1099 as vendor_print_1099,
        v.is_hold_ap_checks as hold_ap_checks,
        v.terms as vendor_terms,
        v.minimum_ap_check,

        -- Entity classification
        case
            when o.id is not null then 'Owner'
            when v.id is not null then 'Vendor'
            else 'Other'
        end as entity_type

    from {{ ref('stg_oda__entity_v2') }} e
    left join {{ ref('stg_oda__owner_v2') }} o on e.id = o.entity_id
    left join {{ ref('stg_oda__vendor_v2') }} v on e.id = v.entity_id
)

select
    *,
    current_timestamp as dim_created_at,
    current_timestamp as dim_updated_at
from entities_base
