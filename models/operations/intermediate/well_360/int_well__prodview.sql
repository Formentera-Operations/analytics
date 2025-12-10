{{
    config(
        materialized='view',
        tags=['well_360', 'source_prep']
    )
}}

{#
    ProdView Well Attributes
    ========================
    Prepares ProdView data for Well 360 integration.
    
    Source Priority: Primary for well name, is_operated, and status (per engineer feedback)
    Deduplication: Uses most recent unit_id per EID
    
    Note: ProdView is unit-centric, so we filter to well-type units only
    Note: Status comes from stg_prodview__status with engineer-approved mapping
    Note: First production date derived from daily allocations (true first HC production)
#}

with units as (
    select 
        "Property EID" as eid,
        "Unit Record ID" as unit_id,
        "Regulatory ID" as regulatory_id,
        "API 10" as api_10,
        "Property Number" as property_number,
        "Combo Curve ID" as combo_curve_id,
        "Cost Center" as cost_center,
        "Lease Name" as lease_name,
        "Unit Name" as unit_name,
        "Operator" as operator_name,
        "Division" as division,
        "AssetCo" as asset_co,
        "Regulatory Field Name" as regulatory_field_name,
        "Field Office" as field_office,
        "Country" as country,
        "State/Province" as state_province,
        "County" as county,
        "Foreman Area" as foreman_area,
        "Route" as route,
        "Facility Name" as facility_name,
        "Pad Name" as pad_name,
        "Completion Status" as completion_status,
        "Current Completion Status" as current_completion_status,  -- FK to status table
        "Producing Method" as producing_method,
        "Surface Latitude" as surface_latitude,
        "Surface Longitude" as surface_longitude,
        "Is Operated" as is_operated,
        "Unit Sub Type" as unit_sub_type
    from {{ ref('stg_prodview__units') }}
    where 
        "Unit Sub Type" ilike '%well%' 
        and "Property EID" is not null 
        and "Completion Status" != 'INACTIVE'
),

-- Get current status from status table with engineer-approved mapping
-- Join key: units."Current Completion Status" -> status."Status Record ID"
status_mapped as (
    select
        "Status Record ID" as status_record_id,
        "Status" as status_raw,
        "Status Date" as status_date,
        -- Engineer-approved status mapping (Parker & team)
        case "Status"
            -- Producing statuses
            when 'Active' then 'Producing'
            when 'Completing' then 'Producing'
            when 'ESP' then 'Producing'
            when 'ESP - OWNED' then 'Producing'
            when 'FLOWING' then 'Producing'
            when 'Flowing' then 'Producing'
            when 'FLOWING - CASING' then 'Producing'
            when 'FLOWING - TUBING' then 'Producing'
            when 'GAS LIFT' then 'Producing'
            when 'Producer' then 'Producing'
            
            -- Shut In statuses
            when 'INACTIVE' then 'Shut In'
            when 'INACTIVE COMPLETED' then 'Shut In'
            when 'INACTIVE INJECTOR' then 'Shut In'
            when 'INACTIVE PRODUCER' then 'Shut In'
            when 'SHUT IN' then 'Shut In'
            when 'Shut-In' then 'Shut In'
            
            -- Injecting
            when 'INJECTING' then 'Injecting'
            
            -- Pass through if not mapped
            else "Status"
        end as status_clean
    from {{ ref('stg_prodview__status') }}
),

-- First production date from daily allocations (true first HC production)
daily_production as (
    select
        "Unit Record ID" as unit_id,
        "Allocation Date" as production_date,
        coalesce("Allocated Oil bbl", 0) + coalesce("Allocated Condensate bbl", 0) + coalesce("Allocated NGL bbl", 0) as total_oil_bbl,
        coalesce("Allocated Gas mcf", 0) as total_gas_mcf
    from {{ ref('stg_prodview__daily_allocations') }}
    where "Allocation Date" is not null
),

first_production as (
    select
        unit_id,
        min(production_date) as first_hc_production_date
    from daily_production
    where total_oil_bbl > 0 or total_gas_mcf > 0
    group by unit_id
),

-- Join units with current status and first production date
source as (
    select 
        u.*,
        s.status_raw as prodview_status_raw,
        s.status_clean as prodview_status_clean,
        s.status_date as prodview_status_date,
        fp.first_hc_production_date as prodview_first_production_date
    from units u
    left join status_mapped s on u.current_completion_status = s.status_record_id
    left join first_production fp on u.unit_id = fp.unit_id
),

deduplicated as (
    select *
    from source
    qualify row_number() over (
        partition by eid 
        order by 
            case when prodview_status_clean is not null then 0 else 1 end,
            unit_id desc
    ) = 1
)

select * from deduplicated