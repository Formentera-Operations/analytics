{{
    config(
        materialized='view',
        tags=['combo_curve', 'forecasting']
    )
}}

-- This model flattens the SEGMENTS array from best_forecast_params
-- Each forecast can have multiple segments, and this creates one row per segment

with staging as (
    
    select * from {{ ref('stg_cc__forecast_outputs') }}
    

),

flattened_segments as (
    
    select
        -- Original forecast identifiers
        forecast_output_id,
        well_id,
        project_id,
        forecast_id,
        product_phase,
        forecast_type,
        forecast_subtype,
        forecast_status,
        
        -- Timestamps
        forecasted_at,
        forecast_date,
        
        -- EUR from the top level
        try_to_number(best_forecast_params:EUR::string, 38, 2) as total_eur,
        
        -- Segment array position
        f.index as segment_index,
        f.value as segment_data,
        
        -- Parse segment fields
        try_to_number(f.value:SEGMENTINDEX::string) as segment_number,
        f.value:SEGMENTTYPE::string as segment_type,
        
        -- ARPS parameters
        try_to_number(f.value:B::string, 38, 4) as b_factor,
        try_to_number(f.value:DINOMINAL::string, 38, 6) as decline_nominal,
        try_to_number(f.value:DIEFFSEC::string, 38, 6) as decline_effective_sec,
        try_to_number(f.value:QSTART::string, 38, 4) as q_start,
        try_to_number(f.value:QEND::string, 38, 4) as q_end,
        
        -- Dates (removing any non-standard characters that might be in the data)
        try_to_date(regexp_replace(f.value:STARTDATE::string, '[^0-9-]', '')) as segment_start_date,
        try_to_date(regexp_replace(f.value:ENDDATE::string, '[^0-9-]', '')) as segment_end_date,
        try_to_date(regexp_replace(f.value:SWDATE::string, '[^0-9-]', '')) as switch_date,
        
        -- Optional fields (may be null)
        try_to_number(f.value:SLOPE::string, 38, 6) as slope,
        try_to_number(f.value:FLATVALUE::string, 38, 4) as flat_value,
        try_to_number(f.value:REALIZEDDSWEFFSEC::string, 38, 6) as realized_dsw_eff_sec,
        try_to_number(f.value:TARGETDSWEFFSEC::string, 38, 6) as target_dsw_eff_sec
        
    from staging,
        lateral flatten(input => best_forecast_params:SEGMENTS) f

),

enriched as (
    
    select
        *,
        
        -- Calculate segment duration
        datediff('day', segment_start_date, segment_end_date) as segment_duration_days,
        datediff('month', segment_start_date, segment_end_date) as segment_duration_months,
        
        -- Decline type classification
        case
            when segment_type = 'arps' and b_factor = 0 then 'Exponential'
            when segment_type = 'arps' and b_factor > 0 and b_factor < 1 then 'Hyperbolic'
            when segment_type = 'arps' and b_factor = 1 then 'Harmonic'
            when segment_type = 'arps' and b_factor > 1 then 'Modified Hyperbolic'
            when segment_type = 'arps_modified' then 'Modified Hyperbolic'
            when segment_type = 'flat' then 'Flat Production'
            when segment_type = 'linear' then 'Linear Decline'
            else segment_type
        end as decline_type,
        
        -- Rate change metrics
        q_start - q_end as rate_decline,
        case 
            when q_start > 0 then ((q_start - q_end) / q_start) * 100
            else null
        end as rate_decline_percentage,
        
        -- Annual effective decline rate (if using monthly nominal)
        case
            when decline_nominal > 0 then 
                (1 - power(1 - decline_nominal, 12)) * 100
            else null
        end as annual_decline_percentage

    from flattened_segments

)

select * from enriched