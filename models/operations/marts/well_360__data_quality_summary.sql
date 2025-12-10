{{
    config(
        materialized='view',
        tags=['well_360', 'data_quality']
    )
}}

{#
    Well 360 Data Quality Dashboard
    ===============================
    
    Aggregated metrics for monitoring Well 360 data health.
    Use this for executive dashboards and data stewardship workflows.
#}

with well_360 as (
    select * from {{ ref('well_360') }}
),

-- Overall counts
totals as (
    select
        count(*) as total_wells,
        count_if(in_oda) as wells_in_oda,
        count_if(in_combo_curve) as wells_in_combo_curve,
        count_if(in_wellview) as wells_in_wellview,
        count_if(in_prodview) as wells_in_prodview
    from well_360
),

-- Source coverage analysis
source_coverage as (
    select
        source_system_count,
        count(*) as well_count,
        round(count(*) * 100.0 / sum(count(*)) over(), 2) as pct_of_total
    from well_360
    group by source_system_count
),

-- Conflict analysis
conflicts as (
    select
        count_if(well_name_conflict) as well_name_conflicts,
        count_if(api_conflict) as api_conflicts,
        count_if(is_operated_conflict) as is_operated_conflicts,
        count_if(location_conflict) as location_conflicts,
        count_if(has_any_conflict) as wells_with_any_conflict,
        round(count_if(has_any_conflict) * 100.0 / count(*), 2) as pct_with_conflicts
    from well_360
),

-- Completeness distribution
completeness as (
    select
        case 
            when completeness_score >= 90 then '90-100 (Excellent)'
            when completeness_score >= 70 then '70-89 (Good)'
            when completeness_score >= 50 then '50-69 (Fair)'
            else '0-49 (Poor)'
        end as completeness_tier,
        count(*) as well_count,
        round(avg(completeness_score), 1) as avg_score
    from well_360
    group by 1
),

-- Field-level completeness
field_completeness as (
    select
        round(count_if(api_10 is not null) * 100.0 / count(*), 1) as api_10_pct,
        round(count_if(well_name is not null) * 100.0 / count(*), 1) as well_name_pct,
        round(count_if(cost_center_number is not null) * 100.0 / count(*), 1) as cost_center_pct,
        round(count_if(company_code is not null) * 100.0 / count(*), 1) as company_code_pct,
        round(count_if(is_operated is not null) * 100.0 / count(*), 1) as is_operated_pct,
        round(count_if(state is not null) * 100.0 / count(*), 1) as state_pct,
        round(count_if(county is not null) * 100.0 / count(*), 1) as county_pct,
        round(count_if(surface_latitude is not null) * 100.0 / count(*), 1) as coordinates_pct,
        round(count_if(well_configuration_type is not null) * 100.0 / count(*), 1) as config_type_pct,
        round(count_if(spud_date is not null) * 100.0 / count(*), 1) as spud_date_pct,
        round(count_if(oda_status is not null or prodview_status is not null) * 100.0 / count(*), 1) as status_pct
    from well_360
),

-- Orphan wells (only in one system)
orphans as (
    select
        source_systems as orphan_source,
        count(*) as orphan_count
    from well_360
    where source_system_count = 1
    group by source_systems
)

select
    -- Summary metrics
    t.total_wells,
    t.wells_in_oda,
    t.wells_in_combo_curve,
    t.wells_in_wellview,
    t.wells_in_prodview,
    
    -- Conflict metrics
    c.wells_with_any_conflict,
    c.pct_with_conflicts,
    c.well_name_conflicts,
    c.api_conflicts,
    c.is_operated_conflicts,
    c.location_conflicts,
    
    -- Field completeness
    f.api_10_pct,
    f.well_name_pct,
    f.cost_center_pct,
    f.company_code_pct,
    f.is_operated_pct,
    f.state_pct,
    f.county_pct,
    f.coordinates_pct,
    f.config_type_pct,
    f.spud_date_pct,
    f.status_pct,
    
    -- Timestamp
    current_timestamp() as generated_at

from totals t
cross join conflicts c
cross join field_completeness f