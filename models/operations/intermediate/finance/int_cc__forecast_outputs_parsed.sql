{{
    config(
        materialized='view',
        tags=['combo_curve', 'forecasting']
    )
}}

-- Example intermediate model showing how to parse VARIANT columns in Snowflake
-- Updated to match actual JSON structure from best_forecast_params and ratio_params
-- 
-- Key insights:
-- 1. Forecasts can be DETERMINISTIC (BEST only) or PROBABILISTIC (P10, P50, P90, BEST)
-- 2. RATIO field contains ratio-based forecasts (GOR, WOR) with similar structure to BEST
-- 3. Dates may contain special characters that need cleaning with REGEXP_REPLACE
-- 4. SEGMENTS array contains decline curve parameters
-- 5. JSON field names are UPPERCASE in Snowflake (converted from lowercase in API)
--
-- Snowflake-specific notes:
-- 1. Use : to navigate JSON paths (e.g., variant_column:path:to:value)
-- 2. Use :: for type casting (e.g., ::string, ::number)
-- 3. Use TRY_TO_* functions instead of TRY_CAST for safe conversions from VARIANT
-- 4. TRY_TO_NUMBER(value::string, precision, scale) for numeric conversions
-- 5. JSON field names are CASE-SENSITIVE in Snowflake

with staging as (
    
    select * from {{ ref('stg_cc__forecast_outputs') }}

),

parsed_variants as (
    
    select
        -- All base columns
        *,
        
        -- Parse BEST forecast parameters (deterministic or probabilistic best case)
        try_to_number(best_forecast_params:EUR::string, 38, 2) as best_eur,
        
        -- Extract first segment from BEST scenario
        try_to_number(best_forecast_params:SEGMENTS[0]:B::string, 38, 4) as best_b_factor,
        try_to_number(best_forecast_params:SEGMENTS[0]:DINOMINAL::string, 38, 6) as best_decline_nominal,
        try_to_number(best_forecast_params:SEGMENTS[0]:DIEFFSEC::string, 38, 6) as best_decline_effective,
        try_to_number(best_forecast_params:SEGMENTS[0]:QSTART::string, 38, 4) as best_initial_rate,
        try_to_number(best_forecast_params:SEGMENTS[0]:QEND::string, 38, 4) as best_end_rate,
        best_forecast_params:SEGMENTS[0]:SEGMENTTYPE::string as best_segment_type,
        try_to_date(regexp_replace(best_forecast_params:SEGMENTS[0]:STARTDATE::string, '[^0-9-]', '')) as best_start_date,
        try_to_date(regexp_replace(best_forecast_params:SEGMENTS[0]:ENDDATE::string, '[^0-9-]', '')) as best_end_date,
        try_to_date(regexp_replace(best_forecast_params:SEGMENTS[0]:SWDATE::string, '[^0-9-]', '')) as best_switch_date,
        try_to_number(best_forecast_params:SEGMENTS[0]:REALIZEDDSWEFFSEC::string, 38, 6) as best_realized_dsw_eff,
        try_to_number(best_forecast_params:SEGMENTS[0]:TARGETDSWEFFSEC::string, 38, 6) as best_target_dsw_eff,
        
        -- Check if probabilistic forecast (has P10, P50, P90)
        case 
            when best_forecast_params:P10 is not null then true 
            else false 
        end as is_probabilistic,
        
        -- Parse P10 scenario (if exists)
        try_to_number(best_forecast_params:P10:EUR::string, 38, 2) as p10_eur,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:QSTART::string, 38, 4) as p10_initial_rate,
        try_to_number(best_forecast_params:P10:SEGMENTS[0]:DINOMINAL::string, 38, 6) as p10_decline_nominal,
        
        -- Parse P50 scenario (if exists)
        try_to_number(best_forecast_params:P50:EUR::string, 38, 2) as p50_eur,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:QSTART::string, 38, 4) as p50_initial_rate,
        try_to_number(best_forecast_params:P50:SEGMENTS[0]:DINOMINAL::string, 38, 6) as p50_decline_nominal,
        
        -- Parse P90 scenario (if exists)
        try_to_number(best_forecast_params:P90:EUR::string, 38, 2) as p90_eur,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:QSTART::string, 38, 4) as p90_initial_rate,
        try_to_number(best_forecast_params:P90:SEGMENTS[0]:DINOMINAL::string, 38, 6) as p90_decline_nominal,
        
        -- Count segments
        array_size(best_forecast_params:SEGMENTS) as best_segment_count,
        
        -- Parse RATIO parameters
        ratio_params:BASEPHASE::string as ratio_base_phase,
        try_to_number(ratio_params:EUR::string, 38, 2) as ratio_eur,
        try_to_number(ratio_params:SEGMENTS[0]:QSTART::string, 38, 4) as ratio_initial_rate,
        try_to_number(ratio_params:SEGMENTS[0]:QEND::string, 38, 4) as ratio_end_rate,
        ratio_params:SEGMENTS[0]:SEGMENTTYPE::string as ratio_segment_type,
        
        -- Parse Type Curve data
        type_curve_data:NAME::string as type_curve_name,
        type_curve_data:TYPE::string as type_curve_type,
        
        -- Parse Type Curve Apply Settings (if exists)
        type_curve_apply_settings:METHOD::string as type_curve_method,
        type_curve_apply_settings:CONFIDENCE_LEVEL::string as type_curve_confidence,
        try_to_number(type_curve_apply_settings:SCALE_FACTOR::string, 38, 4) as type_curve_scale_factor

    from staging

),

enriched as (
    
    select
        *,
        
        -- Add business logic calculations based on actual fields
        case
            when best_decline_nominal > 0.5 then 'HIGH'
            when best_decline_nominal > 0.2 then 'MEDIUM'
            when best_decline_nominal > 0 then 'LOW'
            else 'NONE'
        end as decline_category,
        
        -- Decline type classification based on segment type and b-factor
        case
            when best_segment_type = 'arps' and best_b_factor = 0 then 'Exponential'
            when best_segment_type = 'arps' and best_b_factor > 0 and best_b_factor < 1 then 'Hyperbolic'
            when best_segment_type = 'arps' and best_b_factor = 1 then 'Harmonic'
            when best_segment_type in ('arps', 'arps_modified') and best_b_factor > 1 then 'Modified Hyperbolic'
            when best_segment_type = 'arps_modified' then 'Modified Hyperbolic'
            when best_segment_type = 'flat' then 'Flat Production'
            else best_segment_type
        end as decline_type,
        
        -- Flag for data quality
        case
            when best_initial_rate is null and is_forecasted = true then true
            else false
        end as missing_forecast_data_flag,
        
        -- Create forecast age buckets
        case
            when datediff('day', forecasted_at, current_timestamp()) <= 30 then 'RECENT'
            when datediff('day', forecasted_at, current_timestamp()) <= 90 then 'QUARTER_OLD'
            when datediff('day', forecasted_at, current_timestamp()) <= 365 then 'YEAR_OLD'
            else 'HISTORICAL'
        end as forecast_age_category,
        
        -- Calculate forecast duration
        datediff('year', best_start_date, best_end_date) as forecast_duration_years

    from parsed_variants

)

select * from enriched