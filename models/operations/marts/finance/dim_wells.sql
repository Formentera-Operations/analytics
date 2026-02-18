{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Wells
    
    Purpose: Well/cost center master data with geographic and operational classifications
    Grain: One row per well (well_id)
    
    Use cases:
    - LOS reporting by property
    - Geographic analysis by basin
    - Operated vs non-operated reporting
    - Production status tracking
    
    Sources:
    - stg_oda__wells
    - stg_oda__userfield (for search_key, field)
#}

with wells_base as (
    select
        id,
        code,
        code_sort,
        name,
        api_number,
        legal_description,
        country_name,
        state_code,
        state_name,
        county_name,
        operating_group_code,
        operating_group_name,
        operator_id,
        cost_center_type_code,
        cost_center_type_name,
        well_status_type_code,
        well_status_type_name,
        production_status_name,
        property_reference_code,
        is_stripper_well,
        is_hold_all_billing,
        is_suspend_all_revenue,
        spud_date,
        first_production_date,
        shut_in_date,
        inactive_date,
        nid
    from {{ ref('stg_oda__wells') }}
),

-- Pivot userfields to get search_key and field
userfields as (
    select
        id as well_id,
        max(case when user_field_name = 'UF-SEARCH KEY' then user_field_value_string end) as search_key,
        max(case when user_field_name = 'UF-PV FIELD' then user_field_value_string end) as pv_field,
        max(case when user_field_name = 'UF-OPERATED?' then user_field_value_string end) as uf_operated,
        max(case when user_field_name = 'UF-OPERATOR' then user_field_value_string end) as uf_operator
    from {{ ref('stg_oda__userfield') }}
    where user_field_name in ('UF-SEARCH KEY', 'UF-PV FIELD', 'UF-OPERATED?', 'UF-OPERATOR')
    group by id
),

final as (
    select
        -- =================================================================
        -- Well Identity
        -- =================================================================
        w.id as well_id,
        w.code as well_code,
        w.code_sort,
        w.name as well_name,
        w.api_number,
        w.legal_description,
        w.nid,

        -- =================================================================
        -- Userfield Attributes
        -- =================================================================
        uf.search_key,
        uf.pv_field,


        -- =================================================================
        -- Geography
        -- =================================================================
        w.country_name,
        w.state_code,
        w.state_name,
        w.county_name,

        -- Basin classification based on state/county
        w.operating_group_code,

        -- =================================================================
        -- Operating Group
        -- =================================================================
        w.operating_group_name,
        w.operator_id,
        w.property_reference_code,

        -- =================================================================
        -- Operated Status (from property reference code)
        -- =================================================================
        w.cost_center_type_code,

        w.cost_center_type_name,

        w.well_status_type_code,

        -- =================================================================
        -- Cost Center Classification
        -- =================================================================
        w.well_status_type_name,
        w.production_status_name,

        w.is_stripper_well,

        -- =================================================================
        -- Well Status
        -- =================================================================
        w.is_hold_all_billing as is_hold_billing,
        w.is_suspend_all_revenue as is_suspend_revenue,
        w.spud_date,

        -- Simplified activity status
        w.first_production_date,

        -- =================================================================
        -- Well Type (from naming patterns)
        -- =================================================================
        w.shut_in_date,

        -- =================================================================
        -- Operational Flags
        -- =================================================================
        w.inactive_date,
        case
            -- Permian Basin (Texas)
            when w.state_name = 'Texas' and w.county_name in (
                'ECTOR', 'CRANE', 'WINKLER', 'ANDREWS', 'MARTIN', 'GLASSCOCK',
                'GAINES', 'PECOS', 'REEVES', 'COCHRAN', 'HOCKLEY', 'CROCKETT',
                'STERLING', 'UPTON', 'MIDLAND', 'HOWARD', 'WARD', 'LOVING'
            ) then 'Permian Basin'

            -- Eagle Ford / South Texas
            when w.state_name = 'Texas' and w.county_name in (
                'FRIO', 'ZAVALA', 'DIMMIT', 'KARNES', 'DEWITT', 'GONZALES',
                'LAVACA', 'MCMULLEN', 'LASALLE', 'ATASCOSA', 'WILSON'
            ) then 'Eagle Ford'

            -- Texas Panhandle / Anadarko
            when w.state_name = 'Texas' and w.county_name in (
                'WHEELER', 'HEMPHILL', 'ROBERTS', 'GRAY', 'HUTCHINSON'
            ) then 'Texas Panhandle'

            -- SCOOP/STACK / Anadarko Basin (Oklahoma)
            when w.state_name = 'Oklahoma' and w.county_name in (
                'OKLAHOMA', 'CANADIAN', 'GRADY', 'MCCLAIN', 'LOGAN',
                'GARFIELD', 'KINGFISHER', 'GRANT', 'NOBLE', 'BLAINE',
                'CUSTER', 'CADDO', 'DEWEY', 'MAJOR'
            ) then 'SCOOP/STACK'

            -- Williston Basin / Bakken (North Dakota)
            when w.state_name = 'North Dakota' and w.county_name in (
                'DIVIDE', 'BURKE', 'BOTTINEAU', 'WILLIAMS', 'MOUNTRAIL',
                'MCKENZIE', 'DUNN', 'STARK'
            ) then 'Williston Basin'

            -- Mississippi Interior Salt Basin
            when w.state_name = 'Mississippi' then 'Mississippi'

            -- Louisiana Onshore
            when w.state_name = 'Louisiana' then 'Louisiana'

            -- Marcellus/Utica (Pennsylvania)
            when w.state_name = 'Pennsylvania' then 'Appalachian Basin'

            -- Arkansas
            when w.state_name = 'Arkansas' then 'Arkansas'

            else 'Other'
        end as basin_name,
        case
            when w.property_reference_code = 'NON-OPERATED' then 'NON-OPERATED'
            when w.property_reference_code in ('OPERATED', 'Operated') then 'OPERATED'
            when w.property_reference_code = 'CONTRACT_OP' then 'CONTRACT OPERATED'
            when w.property_reference_code in ('DNU', 'ACCOUNTING', 'Accounting') then 'NON-WELL'
            when w.property_reference_code in ('OTHER', 'Other') then 'OTHER'
            when w.property_reference_code = 'MIDSTREAM' then 'MIDSTREAM'
            else 'UNKNOWN'
        end as op_ref,

        -- Revenue generating (active, producing, not held)
        coalesce(
            -- First: check userfield UF-OPERATED?
            case
                when upper(uf.uf_operated) in ('YES', 'Y', 'TRUE', '1') then true
                when upper(uf.uf_operated) in ('NO', 'N', 'FALSE', '0') then false
            end,
            -- Fallback: derive from property_reference_code
            w.property_reference_code in ('OPERATED', 'Operated', 'CONTRACT_OP')
        ) as is_operated,

        -- =================================================================
        -- Key Dates
        -- =================================================================
        coalesce(w.cost_center_type_name = 'Well', false) as is_well,
        case
            when w.well_status_type_name = 'Producing' and w.production_status_name = 'Active' then 'Producing'
            when w.well_status_type_name = 'Shut In' or w.production_status_name = 'Shutin' then 'Shut In'
            when
                w.well_status_type_name = 'Plugged and Abandoned' or w.production_status_name = 'Plugged'
                then 'Plugged & Abandoned'
            when
                w.well_status_type_name = 'Temp Abandoned' or w.production_status_name = 'Temporarily Abandoned'
                then 'Temporarily Abandoned'
            when w.well_status_type_name = 'Planned' then 'Planned'
            when w.well_status_type_name = 'Injector' then 'Injector'
            when w.well_status_type_name = 'Sold' then 'Sold'
            else 'Other'
        end as activity_status,
        case
            -- Saltwater disposal
            when upper(w.name) like '%SWD%' or upper(w.name) like '%DISPOSAL%' then 'SWD'
            -- Injection well
            when w.well_status_type_name = 'Injector' or upper(w.name) like '%INJ%' then 'Injector'
            -- Horizontal (ends in H, MXH, WXH, MH, etc.)
            when regexp_like(upper(w.name), '.*[0-9]+[MW]?X?H(-[A-Z0-9]+)?$') then 'Horizontal'
            when upper(w.name) like '%H-LL%' or upper(w.name) like '%H-SL%' then 'Horizontal'
            -- Unit wells
            when upper(w.name) like '%UNIT%' then 'Unit Well'
            -- Default for actual wells
            when w.cost_center_type_name = 'Well' then 'Vertical/Conventional'
            else 'Other'
        end as well_type,
        coalesce(
            w.well_status_type_name = 'Producing'
            and w.production_status_name = 'Active'
            and not w.is_hold_all_billing
            and not w.is_suspend_all_revenue,
            false
        ) as is_revenue_generating,

        -- =================================================================
        -- Metadata
        -- =================================================================
        current_timestamp() as _refreshed_at

    from wells_base w
    left join userfields uf
        on w.id = uf.well_id
)

select * from final
