{{ config(
    materialized='table',
    indexes=[
        {'columns': ['state_name', 'county_name']},
        {'columns': ['well_status_type_name']},
        {'columns': ['basin_name']}
    ]
) }}

with wells_enhanced as (
    select 
        id as well_id,
        code as well_code,
        name as well_name,
        api_number,
        state_name,
        county_name,
        operating_group_name,
        operator_id,
        well_status_type_name,
        production_status_name,
        cost_center_type_name,
        stripper_well,
        hold_all_billing,
        suspend_all_revenue,
        spud_date,
        first_production_date,
        shut_in_date,
        
        -- Basin classification based on geography
        case 
            when state_name = 'Texas' and county_name in ('ECTOR', 'CRANE', 'WINKLER', 'ANDREWS', 'REAGAN', 'UPTON') then 'Permian Basin'
            when state_name = 'Texas' and county_name in ('FRIO', 'KARNES', 'DEWITT', 'GONZALES', 'LAVACA') then 'Eagle Ford'
            when state_name = 'Oklahoma' then 'SCOOP/STACK'
            when state_name = 'North Dakota' then 'Bakken'
            when state_name = 'Mississippi' then 'Mississippi'
            when state_name = 'Louisiana' then 'Louisiana'
            when state_name = 'Pennsylvania' then 'Marcellus/Utica'
            else 'Other'
        end as basin_name,
        
        -- Well type classification
        case 
            when upper(name) like '%H%' and (upper(name) like '%MX%' or upper(name) like '%1H%' or upper(name) like '%2H%') then 'Horizontal'
            when upper(name) like '%DISPOSAL%' or upper(name) like '%LINE%' then 'Infrastructure'
            when upper(name) like '%DSU%' then 'Drilling Spacing Unit'
            when cost_center_type_name = 'Well' then 'Conventional Well'
            else 'Other'
        end as well_type,
        
        -- Production activity status
        case 
            when well_status_type_name = 'Producing' and production_status_name = 'Active' then 'Producing'
            when well_status_type_name = 'Shut In' or production_status_name = 'Shutin' then 'Shut In'
            when well_status_type_name = 'Plugged and Abandoned' or production_status_name = 'Plugged' then 'Plugged & Abandoned'
            when well_status_type_name = 'Temp Abandoned' or production_status_name = 'Temporarily Abandoned' then 'Temporarily Abandoned'
            when well_status_type_name = 'Planned' then 'Planned'
            else 'Other'
        end as activity_status,
        
        -- Revenue generating flag
        case 
            when well_status_type_name = 'Producing' 
                 and production_status_name = 'Active'
                 and (hold_all_billing = false or hold_all_billing is null)
                 and (suspend_all_revenue = false or suspend_all_revenue is null)
            then true
            else false
        end as is_revenue_generating,
        
        -- Cost center flag
        case 
            when cost_center_type_name = 'Well' then true
            else false
        end as is_cost_center
        
    from {{ ref('stg_oda__wells') }}
)

select *,
    current_timestamp as dim_created_at,
    current_timestamp as dim_updated_at
from wells_enhanced