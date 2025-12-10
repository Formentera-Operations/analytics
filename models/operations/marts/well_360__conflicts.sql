{{
    config(
        materialized='view',
        tags=['well_360', 'data_quality', 'stewardship']
    )
}}

{#
    Well 360 Conflict Detail
    ========================
    
    Detailed view of wells with cross-system data conflicts.
    Use this for data stewardship workflows and remediation.
    
    Best Practice: Review conflicts weekly and update authoritative source.
#}

with well_360 as (
    select * from {{ ref('well_360') }}
),

oda as (
    select * from {{ ref('int_well__oda') }}
),

cc as (
    select * from {{ ref('int_well__combo_curve') }}
),

wv as (
    select * from {{ ref('int_well__wellview') }}
),

pv as (
    select * from {{ ref('int_well__prodview') }}
)

select
    w.eid,
    w.well_name as golden_well_name,
    w.api_10 as golden_api_10,
    w.is_operated as golden_is_operated,
    w.state as golden_state,
    w.county as golden_county,
    
    -- All source values for comparison
    oda.well_name as oda_well_name,
    cc.well_name as cc_well_name,
    wv.well_name as wv_well_name,
    pv.unit_name as pv_well_name,
    
    oda.api_number as oda_api,
    cc.api_10 as cc_api_10,
    wv.api_10 as wv_api_10,
    pv.api_10 as pv_api_10,
    
    oda.is_operated as oda_is_operated,
    cc.is_operated as cc_is_operated,
    wv.is_operated as wv_is_operated,
    
    oda.state_code as oda_state,
    cc.state as cc_state,
    wv.state_province as wv_state,
    
    oda.county_name as oda_county,
    cc.county as cc_county,
    wv.county_parish as wv_county,
    
    -- Conflict flags
    w.well_name_conflict,
    w.api_conflict,
    w.is_operated_conflict,
    w.location_conflict,
    
    -- Prioritization
    case
        when w.api_conflict then 'HIGH'
        when w.is_operated_conflict then 'HIGH'
        when w.well_name_conflict then 'MEDIUM'
        when w.location_conflict then 'LOW'
    end as priority,
    
    -- Conflict description for ticketing
    array_to_string(
        array_construct_compact(
            iff(w.well_name_conflict, 'Well name mismatch', null),
            iff(w.api_conflict, 'API number mismatch', null),
            iff(w.is_operated_conflict, 'Operated status mismatch', null),
            iff(w.location_conflict, 'State/County mismatch', null)
        ), '; '
    ) as conflict_description,
    
    w.source_systems,
    w.completeness_score

from well_360 w
left join oda on w.eid = oda.eid
left join cc on w.eid = cc.eid
left join wv on w.eid = wv.eid
left join pv on w.eid = pv.eid
where w.has_any_conflict = true
order by 
    case when w.api_conflict then 1 when w.is_operated_conflict then 2 else 3 end,
    w.eid