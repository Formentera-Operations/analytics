{{
    config(
        materialized='view',
        tags=['well_360', 'source_prep']
    )
}}

{#
    Combo Curve Well Attributes
    ===========================
    Prepares Combo Curve data for Well 360 integration.
    
    Source Priority: Primary for reserves/type curve data, API numbers, lat/long
    Deduplication: Uses most recent record per EID (adjust as needed)
#}

with source as (
    select 
        phdwin_id as eid,
        well_id as combo_curve_id,
        aries_id as aries_propnum,
        api_10,
        api_14,
        well_name,
        well_type,
        lease_name,
        operator,
        operator_code,
        operator_cateogry as operator_category,  -- fixing typo from source
        is_operated,
        status as combo_curve_status,
        surface_latitude,
        surface_longitude,
        measured_depth,
        true_vertical_depth,
        lateral_length,
        basin,
        county,
        state,
        reserve_category,
        company_name
    from {{ ref('stg_cc__company_wells') }}
    where phdwin_id is not null
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by eid 
        order by 
            -- Prefer records with more complete data
            case when api_14 is not null then 0 else 1 end,
            case when well_name is not null then 0 else 1 end,
            combo_curve_id desc  -- Most recent ID as tiebreaker
    ) = 1
)

select * from deduplicated