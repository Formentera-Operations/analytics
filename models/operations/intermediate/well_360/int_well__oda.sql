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
#}

with source as (
    select 
        right(w.code, 6) as eid,
        left(w.code, 3) as company_code,
        c.name as company_name,
        w.code as cost_center,
        w.name as well_name,
        w.api_number,
        w.property_reference_code as op_nonop_code,
        case 
            when w.property_reference_code = 'OPERATED' then true
            when w.property_reference_code = 'NON-OPERATED' then false
            else null
        end as is_operated,
        w.country_name,
        w.state_code,
        w.county_name,
        w.legal_description,
        w.stripper_well as is_stripper_well,
        w.well_status_type_name as oda_status
    from {{ ref('stg_oda__wells') }} as w
    left join {{ ref('stg_oda__company_v2') }} as c
        on left(w.code, 3) = c.code
    where 
        w.cost_center_type_code = 'W'
        and left(w.code, 3) != 'ULG'
        and w.code is not null
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by eid 
        order by 
            -- Prefer operated wells, then by cost_center
            case when is_operated = true then 0 else 1 end,
            cost_center
    ) = 1
)

select * from deduplicated