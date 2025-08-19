{{ config(
    enable=false
    materialized='table',
    tags=['marts', 'facts', 'jobs', 'performance']
) }}

with job_phases as (
    select * from {{ ref('stg_wellview__job_program_phases') }}
),

jobs as (
    select * from {{ ref('stg_wellview__jobs') }}
),

problems as (
    select 
        job_program_phase_id,
        sum(problem_duration_net_hours) as total_problem_time_hours,
        sum(problem_cost) as total_problem_cost,
        count(*) as problem_count
    from {{ ref('stg_wellview__job_interval_problems') }}
    group by job_program_phase_id
),

costs as (
    select 
        job_report_id as job_phase_id,
        sum(field_estimate_cost) as total_supply_cost,
        sum(case when ops_category = 'Drilling' then field_estimate_cost else 0 end) as drilling_cost,
        sum(case when ops_category = 'Completion' then field_estimate_cost else 0 end) as completion_cost
    from {{ ref('stg_wellview__daily_costs') }}
    group by job_report_id
),

final as (
    select
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['jp.record_id']) }} as job_performance_fact_key,
        
        -- Dimension keys
        {{ dbt_utils.generate_surrogate_key(['jp.well_id']) }} as well_dim_key,
        {{ dbt_utils.generate_surrogate_key(['jp.parent_record_id']) }} as job_dim_key,
        {{ dbt_utils.generate_surrogate_key([
            'extract(year from jp.actual_start_date)',
            'extract(month from jp.actual_start_date)',
            'extract(day from jp.actual_start_date)'
        ]) }} as start_date_dim_key,
        
        -- Natural keys
        jp.record_id as job_phase_id,
        jp.well_id,
        jp.parent_record_id as job_id,
        
        -- Phase information
        jp.combined_phase_types as phase_code,
        jp.description as phase_description,
        
        -- Time metrics (converted to standard units)
        jp.actual_duration_days,
        jp.planned_likely_duration_days as planned_duration_days,
        jp.duration_variance_days,
        jp.duration_variance_days / nullif(jp.planned_likely_duration_days, 0) * 100 as duration_variance_percent,
        
        -- Depth metrics (in feet)
        jp.actual_start_depth_ft,
        jp.actual_end_depth_ft,
        jp.actual_depth_progress_ft,
        jp.planned_depth_progress_ft,
        jp.actual_depth_progress_ft - jp.planned_depth_progress_ft as depth_variance_ft,
        (jp.actual_depth_progress_ft - jp.planned_depth_progress_ft) / nullif(jp.planned_depth_progress_ft, 0) * 100 as depth_variance_percent,
        
        -- Cost metrics (in original currency)
        jp.actual_phase_field_est as actual_phase_cost,
        jp.planned_likely_phase_cost as planned_phase_cost,
        jp.cost_variance_ml as cost_variance,
        jp.cost_variance_ml / nullif(jp.planned_likely_phase_cost, 0) * 100 as cost_variance_percent,
        coalesce(c.total_supply_cost, 0) as supply_cost,
        coalesce(c.drilling_cost, 0) as drilling_cost,
        coalesce(c.completion_cost, 0) as completion_cost,
        
        -- Drilling specific metrics
        jp.drilling_time_hours as total_drilling_time_hours,
        jp.circulating_time_hours as total_circulation_time_hours,
        jp.tripping_time_hours as total_trip_time_hours,
        jp.sliding_time_hours as total_sliding_time_hours,
        jp.rotating_time_hours as total_rotating_time_hours,
        -- Calculate percent time drilling from available columns
        case 
            when jp.time_log_total_hours > 0 
            then jp.drilling_time_hours / jp.time_log_total_hours * 100 
            else null 
        end as percent_time_drilling,
        jp.percent_time_rotating as percent_time_rotating,
        jp.percent_time_sliding as percent_time_sliding,
        
        -- ROP metrics
        jp.rop_ft_per_hour as average_rop_ft_per_hour,
        1 / nullif(jp.rop_instantaneous_avg_min_per_ft, 0) * 60 as instantaneous_average_rop_ft_per_hour,
        jp.rop_rotating_ft_per_hour as rotating_rop_ft_per_hour,
        jp.rop_sliding_ft_per_hour as sliding_rop_ft_per_hour,
        
        -- Problem time metrics
        jp.problem_time_hours as phase_problem_time_hours,
        jp.percent_problem_time as problem_time_percentage,
        jp.time_log_minus_problem_hours as non_problem_time_hours,
        coalesce(p.total_problem_time_hours, 0) as detailed_problem_time_hours,
        coalesce(p.total_problem_cost, 0) as problem_cost,
        coalesce(p.problem_count, 0) as problem_count,
        
        -- Mud metrics
        jp.min_mud_density_lb_per_gal as mud_density_min_ppg,
        jp.max_mud_density_lb_per_gal as mud_density_max_ppg,
        jp.mud_type,
        jp.mud_added_volume_bbl as mud_volume_added_bbl,
        jp.mud_losses_volume_bbl as mud_volume_lost_bbl,
        jp.phase_mud_cost,
        jp.phase_mud_cost_per_depth_per_ft as phase_mud_cost_per_foot,
        
        -- Performance ratios and efficiency
        case 
            when jp.planned_likely_duration_days > 0 
            then jp.actual_duration_days / jp.planned_likely_duration_days 
            else null 
        end as duration_efficiency_ratio,
        
        case 
            when jp.planned_likely_phase_cost > 0 
            then jp.actual_phase_field_est / jp.planned_likely_phase_cost 
            else null 
        end as cost_efficiency_ratio,
        
        case 
            when jp.actual_depth_progress_ft > 0 and jp.actual_duration_days > 0
            then jp.actual_depth_progress_ft / jp.actual_duration_days 
            else null 
        end as feet_per_day,
        
        case 
            when jp.actual_depth_progress_ft > 0 and jp.actual_phase_field_est > 0
            then jp.actual_phase_field_est / jp.actual_depth_progress_ft 
            else null 
        end as cost_per_foot,
        
        -- Operational flags
        jp.is_plan_change as had_plan_change,
        jp.exclude_from_calculations as is_excluded,
        jp.is_definitive as is_definitive,
        case when jp.problem_time_hours > 0 then true else false end as had_problems,
        
        -- Formation and geological info
        jp.formation,
        jp.top_inclination_degrees as inclination_start_degrees,
        jp.bottom_inclination_degrees as inclination_end_degrees,
        jp.max_inclination_degrees,
        
        -- Date information
        jp.actual_start_date,
        jp.actual_end_date,
        jp.planned_likely_start_date as planned_start_date,
        jp.planned_likely_end_date as planned_end_date,
        jp.days_from_spud,
        jp.report_day,
        
        -- Job context from parent job
        j.primary_job_type,
        j.secondary_job_type,
        j.job_objective as job_objective,
        j.client_operator,
        j.responsible_group_1 as primary_contractor,
        j.afe_number,
        j.currency_code,
        
        -- Metadata
        current_timestamp as dbt_updated_at,
        jp.created_at as source_created_at

    from job_phases jp
    left join jobs j 
        on jp.parent_record_id = j.job_id
    left join problems p 
        on jp.record_id = p.job_program_phase_id
    left join costs c 
        on jp.record_id = c.job_phase_id
    
    where jp.actual_start_date is not null  -- Only include phases that actually started
)

select * from final