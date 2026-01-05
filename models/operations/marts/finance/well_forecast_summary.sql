{{
    config(
        materialized='table',
        tags=['combo_curve', 'forecasting', 'summary']
    )
}}

-- Summary table aggregating forecast metrics at the well level
-- Useful for understanding the latest forecast status and key metrics per well

with parsed_forecasts as (
    
    select * from {{ ref('int_cc__forecast_outputs_parsed') }}

),

latest_forecast_per_well_phase as (
    
    -- Get the most recent forecast for each well and phase combination
    select
        well_id,
        product_phase,
        forecast_id,
        forecast_output_id,
        forecasted_at,
        forecast_status,
        best_eur,
        best_initial_rate,
        best_decline_nominal,
        best_b_factor,
        decline_type,
        best_start_date,
        best_end_date,
        forecast_duration_years,
        row_number() over (
            partition by well_id, product_phase 
            order by forecasted_at desc
        ) as rn
    from parsed_forecasts
    where is_forecasted = true

),

well_summary as (
    
    select
        well_id,
        
        -- Count of phases forecasted
        count(distinct product_phase) as phases_forecasted,
        
        -- Latest forecast date across all phases
        max(forecasted_at) as latest_forecast_date,
        min(forecasted_at) as earliest_forecast_date,
        
        -- Aggregate EUR across phases (latest forecasts only)
        sum(case when rn = 1 then best_eur else 0 end) as total_eur_all_phases,
        
        -- Phase-specific EURs (latest forecasts)
        sum(case when rn = 1 and product_phase = 'OIL' then best_eur else 0 end) as oil_eur,
        sum(case when rn = 1 and product_phase = 'GAS' then best_eur else 0 end) as gas_eur,
        sum(case when rn = 1 and product_phase = 'WATER' then best_eur else 0 end) as water_eur,
        
        -- Initial rates by phase (latest forecasts)
        max(case when rn = 1 and product_phase = 'OIL' then best_initial_rate end) as oil_initial_rate,
        max(case when rn = 1 and product_phase = 'GAS' then best_initial_rate end) as gas_initial_rate,
        max(case when rn = 1 and product_phase = 'WATER' then best_initial_rate end) as water_initial_rate,
        
        -- Decline parameters for oil (most common primary phase)
        max(case when rn = 1 and product_phase = 'OIL' then best_decline_nominal end) as oil_decline_nominal,
        max(case when rn = 1 and product_phase = 'OIL' then best_b_factor end) as oil_b_factor,
        max(case when rn = 1 and product_phase = 'OIL' then decline_type end) as oil_decline_type,
        
        -- Forecast status summary (ordered alphabetically by phase:status)
        listagg(distinct 
            case when rn = 1 then product_phase || ':' || forecast_status end, 
            ', '
        ) as phase_status_summary,
        
        -- Check if all phases are approved
        case 
            when count(case when rn = 1 and forecast_status != 'APPROVED' then 1 end) = 0 
            then true 
            else false 
        end as all_phases_approved,
        
        -- Production window
        min(case when rn = 1 then best_start_date end) as production_start_date,
        max(case when rn = 1 then best_end_date end) as production_end_date,
        
        -- Forecast count
        count(distinct forecast_id) as total_forecast_versions,
        count(distinct case when forecast_status = 'APPROVED' then forecast_id end) as approved_forecast_count

    from latest_forecast_per_well_phase
    group by well_id

),

final as (
    
    select
        ws.*,
        
        -- Calculate additional metrics
        datediff('year', production_start_date, production_end_date) as total_production_years,
        datediff('day', latest_forecast_date, current_timestamp()) as days_since_last_forecast,
        
        -- Categorize wells
        case
            when total_eur_all_phases > 1000000 then 'HIGH_EUR'
            when total_eur_all_phases > 500000 then 'MEDIUM_EUR'
            when total_eur_all_phases > 100000 then 'LOW_EUR'
            else 'MARGINAL'
        end as eur_category,
        
        case
            when oil_initial_rate > 1000 then 'HIGH_RATE'
            when oil_initial_rate > 500 then 'MEDIUM_RATE'
            when oil_initial_rate > 100 then 'LOW_RATE'
            else 'MARGINAL_RATE'
        end as initial_rate_category,
        
        -- Audit fields
        current_timestamp() as dbt_updated_at
        
    from well_summary ws

)

select * from final