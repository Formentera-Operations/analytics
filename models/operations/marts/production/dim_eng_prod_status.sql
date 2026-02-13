{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'dim']
) }}

with prodstatus as (
    select *
    from {{ ref('int_dim_prod_status') }}
)

select
    s.calc_lost_production as "Calculate Lost Production",
    s.comments as "Comment",
    s.commingled as "Commingled",
    s.completion_type as "Completion Type",
    s.created_at_utc as "Created At (UTC)",
    s.created_by as "Created By",
    s.flow_direction as "Flow Direction",
    s.id_flownet as "Flow Net ID",
    s.include_in_well_count as "Include In Well Count",
    s.modified_at_utc as "Last Mod At (UTC)",
    s.modified_by as "Last Mod By",
    s.oil_or_condensate as "Oil Or Condensate",
    s.primary_fluid_type as "Primary Fluid Type",
    s.status_clean as "Prod Status",
    s.production_method as "Production Method",
    s.status_date as "Status Date",
    s.id_rec_parent as "Status Parent Record ID",
    s.status as "Status",
    s.id_rec as "Status Record ID"
from prodstatus s
