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
        property_eid as eid,
        id_rec as unit_id,
        regulatory_id,
        api_10,
        property_number,
        combo_curve_id,
        cost_center,
        lease_name,
        unit_name,
        operator_name,
        division,
        asset_company as asset_co,
        regulatory_field_name,
        field_office,
        country,
        state_province,
        county,
        foreman_area,
        route,
        facility_name,
        pad_name,
        completion_status,
        current_completion_status_id as current_completion_status,  -- FK to status table
        producing_method,
        surface_latitude,
        surface_longitude,
        is_operated,
        unit_sub_type
    from {{ ref('stg_prodview__units') }}
    where
        unit_sub_type ilike '%well%'
        and property_eid is not null
        and completion_status != 'INACTIVE'
),

-- Get current status from status table with engineer-approved mapping
-- Join key: units."Current Completion Status" -> status.id_rec
-- Uses normalize_well_status UDF for canonical UPPER_SNAKE_CASE output
status_mapped as (
    select
        id_rec as status_record_id,
        status as status_raw,
        status_date,
        {{ function('normalize_well_status') }}(status) as status_clean
    from {{ ref('stg_prodview__status') }}
),

-- First production date from daily allocations (true first HC production)
daily_production as (
    select
        id_rec_unit as unit_id,
        allocation_date as production_date,
        coalesce(allocated_oil_bbl, 0)
        + coalesce(allocated_condensate_bbl, 0)
        + coalesce(allocated_ngl_bbl, 0) as total_oil_bbl,
        coalesce(allocated_gas_mcf, 0) as total_gas_mcf
    from {{ ref('stg_prodview__daily_allocations') }}
    where allocation_date is not null
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
