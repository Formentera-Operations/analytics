{{
    config(
        materialized='view',
        tags=['well_360', 'spine']
    )
}}

{#
    Well Spine Model
    ================
    Creates the authoritative list of all well EIDs across source systems.
    
    Design Decisions:
    - UNION (not UNION ALL) ensures unique EIDs
    - Each source filtered to valid wells only
    - Source tracking enables debugging orphan/match analysis
    - API-10 captured for Enverus matching (Enverus doesn't have EID)
#}

with combo_curve_wells as (
    select
        phdwin_id as eid,
        api_10,
        'combo_curve' as source_system
    from {{ ref('stg_cc__company_wells') }}
    where phdwin_id is not null
),

wellview_wells as (
    select
        eid,
        api_10_number as api_10,
        'wellview' as source_system
    from {{ ref('stg_wellview__well_header') }}
    where len(eid) = 6
),

prodview_wells as (
    select
        property_eid as eid,
        api_10,
        'prodview' as source_system
    from {{ ref('stg_prodview__units') }}
    where
        unit_sub_type ilike '%well%'
        and property_eid is not null
        and completion_status != 'INACTIVE'
),

oda_wells as (
    select
        api_number as api_10,
        'oda' as source_system,
        right(code, 6) as eid
    from {{ ref('stg_oda__wells') }}
    where
        cost_center_type_code = 'W'
        and left(code, 3) != 'ULG'
        and code is not null
),

-- Union all sources, keeping track of where each EID appears
all_sources as (
    select
        eid,
        api_10,
        source_system
    from combo_curve_wells
    union all
    select
        eid,
        api_10,
        source_system
    from wellview_wells
    union all
    select
        eid,
        api_10,
        source_system
    from prodview_wells
    union all
    select
        eid,
        api_10,
        source_system
    from oda_wells
),

-- Aggregate to get unique EIDs with source presence flags and best API-10
spine as (
    select
        eid,
        -- Get first non-null API-10 for Enverus matching
        max(case when source_system = 'oda' then 1 else 0 end)::boolean as in_oda,
        max(case when source_system = 'combo_curve' then 1 else 0 end)::boolean as in_combo_curve,
        max(case when source_system = 'wellview' then 1 else 0 end)::boolean as in_wellview,
        max(case when source_system = 'prodview' then 1 else 0 end)::boolean as in_prodview,
        max(api_10) as api_10_for_enverus_match,
        count(distinct source_system) as source_system_count,
        listagg(distinct source_system, ', ') within group (
            order by source_system
        ) as source_systems
    from all_sources
    group by eid
)

select * from spine
