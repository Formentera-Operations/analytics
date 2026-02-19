{{
    config(
        materialized='view',
        tags=['well_360', 'source_prep']
    )
}}

{#
    ODA (OnDemand Accounting) Well Attributes
    ============================
    Prepares ODA data for Well 360 integration.

    Source Priority: PRIMARY for accounting truth - cost center, company, op/non-op
    Deduplication: Should be 1:1 but defensive dedup included

    ODA is the system of record for financial/accounting attributes

    Expanded 2026-02-19:
    - Added well_id (ODA internal numeric ID — critical for fct_los joins)
    - Added state_name, production_status_name (for basin_name + is_revenue_generating derivation)
    - Added is_hold_all_billing, is_suspend_all_revenue (financial control flags)
    - Added cost_center_type_code, cost_center_type_name (cost center classification)
    - Added operating_group_code, operating_group_name (operating group hierarchy)
    - Added shut_in_date, inactive_date (key operational dates)
    - Added search_key, pv_field (pivoted from stg_oda__userfield EAV table)
#}

with source as (
    select
        c.name as company_name,
        w.id as well_id,                         -- ODA internal numeric ID (critical for fct_los joins)
        w.code as cost_center,
        w.name as well_name,
        w.api_number,
        w.property_reference_code as op_nonop_code,
        w.country_name,
        w.state_code,
        w.state_name,
        w.county_name,
        w.legal_description,
        w.is_stripper_well,
        w.well_status_type_name as oda_status,
        w.production_status_name,
        w.is_hold_all_billing,
        w.is_suspend_all_revenue,
        w.cost_center_type_code,
        w.cost_center_type_name,
        w.operating_group_code,
        w.operating_group_name,
        w.shut_in_date,
        w.inactive_date,
        right(w.code, 6) as eid,
        left(w.code, 3) as company_code,
        case
            when w.property_reference_code = 'OPERATED' then true
            when w.property_reference_code = 'NON-OPERATED' then false
        end as is_operated
    from {{ ref('stg_oda__wells') }} as w
    left join {{ ref('stg_oda__company_v2') }} as c
        on left(w.code, 3) = c.code
    where
        w.cost_center_type_code = 'W'
        and left(w.code, 3) != 'ULG'
        and w.code is not null
),

-- Dedup userfields per (id, user_field_name) by most recent sync timestamp
-- Using _flow_published_at DESC (NOT user_field_identity — not monotonic with sync order)
userfields_raw as (
    select
        id as well_id,
        case
            when user_field_name = 'UF-SEARCH KEY' then user_field_value_string
        end as search_key_raw,
        case
            when user_field_name = 'UF-PV FIELD' then user_field_value_string
        end as pv_field_raw
    from {{ ref('stg_oda__userfield') }}
    where user_field_name in ('UF-SEARCH KEY', 'UF-PV FIELD')
    qualify row_number() over (
        partition by id, user_field_name
        order by _flow_published_at desc
    ) = 1
),

-- Pivot to one row per well
userfields as (
    select
        well_id,
        max(search_key_raw) as search_key,
        max(pv_field_raw) as pv_field
    from userfields_raw
    group by well_id
),

deduplicated as (
    select
        s.*,
        uf.search_key,
        uf.pv_field
    from source s
    left join userfields uf on s.well_id = uf.well_id
    qualify row_number() over (
        partition by s.eid
        order by
            -- Prefer operated wells, then by cost_center
            case when s.is_operated = true then 0 else 1 end,
            s.cost_center
    ) = 1
)

select * from deduplicated
