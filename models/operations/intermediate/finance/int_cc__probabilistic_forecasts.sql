{{
    config(
        materialized='view',
        tags=['combo_curve', 'forecasting', 'probabilistic']
    )
}}

-- This model handles both deterministic and probabilistic forecasts
-- Probabilistic forecasts have P10, P50, P90 scenarios in addition to BEST
-- Deterministic forecasts only have BEST

with staging as (
    
    select * from {{ ref('stg_cc__forecast_outputs') }}
    where is_forecasted = true  -- Only process forecasted records

),

forecast_scenarios as (
    
    select
        -- Base identifiers
        forecast_output_id,
        well_id,
        project_id,
        forecast_id,
        product_phase,
        forecast_type,
        forecast_subtype,
        forecast_status,
        data_frequency,
        is_forecasted,
        
        -- Timestamps
        forecasted_at,
        forecasted_by_user,
        reviewed_at,
        reviewed_by_user,
        forecast_date,
        
        -- Check forecast type
        case 
            when best_forecast_params:P10 is not null 
                or best_forecast_params:P50 is not null 
                or best_forecast_params:P90 is not null 
            then 'PROBABILISTIC'
            else 'DETERMINISTIC'
        end as forecast_model_type,
        
        -- BEST scenario (always present when forecasted = true)
        try_to_number(best_forecast_params:EUR::string, 38, 2) as best_eur,
        try_to_number(best_forecast_params:SEGMENTS[0]:B::string, 38, 4) as best_b_factor,
        try_to_number(best_forecast_params:SEGMENTS[0]:DINOMINAL::string, 38, 6) as best_di_nominal,
        try_to_number(best_forecast_params:SEGMENTS[0]:DIEFFSEC::string, 38, 6) as best_di_eff_sec,
        try_to_number(best_forecast_params:SEGMENTS[0]:QSTART::string, 38, 4) as best_q_start,
        try_to_number(best_forecast_params:SEGMENTS[0]:QEND::string, 38, 4) as best_q_end,
        best_forecast_params:SEGMENTS[0]:SEGMENTTYPE::string as best_segment_type,
        -- Clean dates by removing any non-standard characters
        try_to_date(regexp_replace(best_forecast_params:SEGMENTS[0]:STARTDATE::string, '[^0-9-]', '')) as best_start_date,
        try_to_date(regexp_replace(best_forecast_params:SEGMENTS[0]:ENDDATE::string, '[^0-9-]', '')) as best_end_date,
        try_to_date(regexp_replace(best_forecast_params:SEGMENTS[0]:SWDATE::string, '[^0-9-]', '')) as best_switch_date,
        try_to_number(best_forecast_params:SEGMENTS[0]:REALIZEDDSWEFFSEC::string, 38, 6) as best_realized_dsw,
        
        -- P10 scenario (high case - 10% probability of exceeding)
        try_to_number(best_forecast_params:P10:EUR::string, 38, 2) as p10_eur,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:B::string, 38, 4) as p10_b_factor,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:DINOMINAL::string, 38, 6) as p10_di_nominal,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:DIEFFSEC::string, 38, 6) as p10_di_eff_sec,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:QSTART::string, 38, 4) as p10_q_start,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:QEND::string, 38, 4) as p10_q_end,
        try_to_date(regexp_replace(best_forecast_params:P10:SEGMENTS[0]:SWDATE::string, '[^0-9-]', '')) as p10_switch_date,
        
        -- P50 scenario (median case - 50% probability)
        try_to_number(best_forecast_params:P50:EUR::string, 38, 2) as p50_eur,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:B::string, 38, 4) as p50_b_factor,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:DINOMINAL::string, 38, 6) as p50_di_nominal,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:DIEFFSEC::string, 38, 6) as p50_di_eff_sec,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:QSTART::string, 38, 4) as p50_q_start,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:QEND::string, 38, 4) as p50_q_end,
        try_to_date(regexp_replace(best_forecast_params:P50:SEGMENTS[0]:SWDATE::string, '[^0-9-]', '')) as p50_switch_date,
        
        -- P90 scenario (low case - 90% probability of exceeding)
        try_to_number(best_forecast_params:P90:EUR::string, 38, 2) as p90_eur,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:B::string, 38, 4) as p90_b_factor,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:DINOMINAL::string, 38, 6) as p90_di_nominal,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:DIEFFSEC::string, 38, 6) as p90_di_eff_sec,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:QSTART::string, 38, 4) as p90_q_start,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:QEND::string, 38, 4) as p90_q_end,
        try_to_date(regexp_replace(best_forecast_params:P90:SEGMENTS[0]:SWDATE::string, '[^0-9-]', '')) as p90_switch_date,
        
        -- RATIO parameters (for ratio-based forecasts like GOR, WOR)
        ratio_params:BASEPHASE::string as ratio_base_phase,
        try_to_number(ratio_params:EUR::string, 38, 2) as ratio_eur,
        try_to_number(ratio_params:SEGMENTS[0]:QSTART::string, 38, 4) as ratio_q_start,
        try_to_number(ratio_params:SEGMENTS[0]:QEND::string, 38, 4) as ratio_q_end,
        ratio_params:SEGMENTS[0]:SEGMENTTYPE::string as ratio_segment_type,
        
        -- Type curve data
        type_curve_data:NAME::string as type_curve_name,
        type_curve_data:TYPE::string as type_curve_type

    from staging

),

enriched as (
    
    select
        *,
        
        -- Calculate uncertainty ranges
        p10_eur - p90_eur as eur_uncertainty_range,
        case 
            when p50_eur > 0 then ((p10_eur - p90_eur) / p50_eur) * 100 
            else null 
        end as eur_uncertainty_percentage,
        
        -- EUR risk adjustments
        coalesce(p50_eur, best_eur) as risked_eur,  -- Use P50 if available, otherwise BEST
        
        -- Decline type classification
        case
            when best_segment_type = 'arps' and best_b_factor = 0 then 'Exponential'
            when best_segment_type = 'arps' and best_b_factor > 0 and best_b_factor < 1 then 'Hyperbolic'
            when best_segment_type = 'arps' and best_b_factor = 1 then 'Harmonic'
            when best_segment_type = 'arps' and best_b_factor > 1 then 'Modified Hyperbolic'
            when best_segment_type = 'arps_modified' then 'Modified Hyperbolic'
            when best_segment_type = 'flat' then 'Flat Production'
            else best_segment_type
        end as decline_type,
        
        -- Production duration
        datediff('day', best_start_date, best_end_date) as production_days,
        datediff('year', best_start_date, best_end_date) as production_years,
        
        -- Time to switch (for modified hyperbolic)
        datediff('day', best_start_date, best_switch_date) as days_to_switch,
        datediff('year', best_start_date, best_switch_date) as years_to_switch,
        
        -- Rate decline metrics
        best_q_start - best_q_end as best_total_decline,
        case 
            when best_q_start > 0 then ((best_q_start - best_q_end) / best_q_start) * 100
            else null
        end as best_decline_percentage,
        
        -- Annual effective decline (converting monthly nominal if needed)
        case
            when data_frequency = 'MONTHLY' and best_di_nominal > 0 then 
                (1 - power(1 - best_di_nominal, 12)) * 100
            when data_frequency = 'DAILY' and best_di_nominal > 0 then
                (1 - power(1 - best_di_nominal, 365)) * 100
            else best_di_nominal * 100
        end as best_annual_decline_percent,
        
        -- P10/P90 ratio for uncertainty assessment
        case 
            when p90_eur > 0 then p10_eur / p90_eur 
            else null 
        end as p10_p90_ratio,
        
        -- Forecast quality flags
        case
            when forecast_model_type = 'PROBABILISTIC' and p10_eur = p50_eur and p50_eur = p90_eur 
            then 'SUSPICIOUS_IDENTICAL_SCENARIOS'
            when forecast_model_type = 'PROBABILISTIC' and p10_eur < p90_eur 
            then 'INVERTED_PROBABILISTIC_ORDER'
            when best_eur is null and is_forecasted = true 
            then 'MISSING_BEST_CASE'
            else 'OK'
        end as data_quality_flag

    from forecast_scenarios 

)

select * from enriched
where forecast_model_type = 'PROBABILISTIC'