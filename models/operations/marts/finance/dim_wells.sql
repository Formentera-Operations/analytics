{{
    config(
        materialized='view',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Wells (view over well_360)

    Purpose: Backward-compatible well dimension for LOS reporting and Cortex Analyst.
    Grain: One row per well with an ODA internal ID (oda_well_id IS NOT NULL).

    ⚠️  DO NOT DELETE OR ALTER COLUMN NAMES — this view is referenced by the live
    Cortex Analyst semantic model: LOS_SEMANTIC_MODEL-2.yaml
    Join keys: FCT_LOS.WELL_ID = DIM_WELLS.WELL_ID (ODA internal numeric ID)

    Architecture:
    - This model is now a thin view over well_360 (the canonical well dimension)
    - All logic is maintained in well_360 / int_well__oda — NOT here
    - Filters to ODA wells only (oda_well_id IS NOT NULL) to preserve the
      pre-existing grain expected by Cortex consumers
    - Columns aliased for backward compatibility with existing queries

    Sources (via well_360):
    - stg_oda__wells (primary)
    - stg_oda__userfield (search_key, pv_field)
    - All other well_360 source systems (WellView, ProdView, ComboCurve, Enverus)
#}

select
    -- Primary key for Cortex Analyst joins (ODA internal numeric ID)
    oda_well_id as well_id,

    -- Well identity
    cost_center_number as well_code,
    well_name,
    api_10 as api_number,

    -- Geography
    basin_name,
    state as state_name,
    county as county_name,

    -- Operational classification
    op_ref,
    is_operated,
    is_revenue_generating,
    is_stripper_well,
    is_well,
    activity_status,
    well_type_oda as well_type,

    -- Userfield attributes (ODA custom fields)
    search_key,
    pv_field,

    -- ODA organizational hierarchy
    operating_group_name,
    cost_center_type_name,

    -- Key dates
    spud_date,
    first_production_date,
    shut_in_date,
    inactive_date,

    -- Metadata
    current_timestamp() as _refreshed_at

from {{ ref('well_360') }}
where oda_well_id is not null
