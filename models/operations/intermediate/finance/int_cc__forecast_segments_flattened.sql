{{
    config(
        materialized='view',
        tags=['combo_curve', 'forecasting']
    )
}}

-- This model flattens the SEGMENTS array from both best_forecast_params (rate forecasts) 
-- and ratio_params (ratio forecasts)
-- Each forecast can have multiple segments, and this creates one row per segment

with staging as (

    select * from {{ ref('stg_cc__forecast_outputs') }}

),

-- Handle rate forecasts with best_forecast_params
rate_forecast_segments as (

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
        forecasted_at::date as forecast_date,

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
        try_to_number(f.value:TARGETDSWEFFSEC::string, 38, 6) as target_dsw_eff_sec,

        -- Ratio-specific fields (will be null for rate forecasts)
        null as base_phase,
        null as ratio_value,

        -- Source indicator
        'rate' as forecast_source

    from staging,
        lateral flatten(input => best_forecast_params:SEGMENTS, outer => false) f
    where
        best_forecast_params is not null
        and best_forecast_params != 'null'
        and forecast_type = 'rate'

),

-- Handle ratio forecasts with ratio_params
ratio_forecast_segments as (

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
        forecasted_at::date as forecast_date,

        -- EUR from the ratio_params
        try_to_number(ratio_params:EUR::string, 38, 2) as total_eur,

        -- Segment array position
        f.index as segment_index,
        f.value as segment_data,

        -- Parse segment fields
        try_to_number(f.value:SEGMENTINDEX::string) as segment_number,
        f.value:SEGMENTTYPE::string as segment_type,

        -- ARPS parameters (may be null for ratio forecasts)
        try_to_number(f.value:B::string, 38, 4) as b_factor,
        try_to_number(f.value:DINOMINAL::string, 38, 6) as decline_nominal,
        try_to_number(f.value:DIEFFSEC::string, 38, 6) as decline_effective_sec,
        try_to_number(f.value:QSTART::string, 38, 4) as q_start,
        try_to_number(f.value:QEND::string, 38, 4) as q_end,

        -- Dates
        try_to_date(regexp_replace(f.value:STARTDATE::string, '[^0-9-]', '')) as segment_start_date,
        try_to_date(regexp_replace(f.value:ENDDATE::string, '[^0-9-]', '')) as segment_end_date,
        try_to_date(regexp_replace(f.value:SWDATE::string, '[^0-9-]', '')) as switch_date,

        -- Optional fields
        try_to_number(f.value:SLOPE::string, 38, 6) as slope,
        try_to_number(f.value:FLATVALUE::string, 38, 4) as flat_value,
        try_to_number(f.value:REALIZEDDSWEFFSEC::string, 38, 6) as realized_dsw_eff_sec,
        try_to_number(f.value:TARGETDSWEFFSEC::string, 38, 6) as target_dsw_eff_sec,

        -- Ratio-specific fields
        ratio_params:BASEPHASE::string as base_phase,
        try_to_number(f.value:RATIOVALUE::string, 38, 6) as ratio_value,

        -- Source indicator
        'ratio' as forecast_source

    from staging,
        lateral flatten(input => ratio_params:SEGMENTS, outer => false) f
    where
        ratio_params is not null
        and ratio_params != 'null'
        and forecast_type = 'ratio'

),

-- Union both types of forecasts
all_segments as (

    select * from rate_forecast_segments

    union all

    select * from ratio_forecast_segments

),

-- Also include rows that don't have segments (if needed)
-- This handles cases where neither best_forecast_params nor ratio_params have SEGMENTS
no_segments as (

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
        forecasted_at::date as forecast_date,

        -- EUR - try both sources
        coalesce(
            try_to_number(best_forecast_params:EUR::string, 38, 2),
            try_to_number(ratio_params:EUR::string, 38, 2)
        ) as total_eur,

        -- No segment data
        0 as segment_index,
        null as segment_data,
        null as segment_number,
        null as segment_type,
        null as b_factor,
        null as decline_nominal,
        null as decline_effective_sec,
        null as q_start,
        null as q_end,
        null as segment_start_date,
        null as segment_end_date,
        null as switch_date,
        null as slope,
        null as flat_value,
        null as realized_dsw_eff_sec,
        null as target_dsw_eff_sec,

        -- Ratio-specific fields
        case
            when forecast_type = 'ratio' then ratio_params:BASEPHASE::string
        end as base_phase,
        null as ratio_value,

        -- Source indicator
        case
            when best_forecast_params is not null and best_forecast_params != 'null' then 'rate_no_segments'
            when ratio_params is not null and ratio_params != 'null' then 'ratio_no_segments'
            else 'no_params'
        end as forecast_source

    from staging
    where
        -- Include rows that don't have segments in either parameter type
        (
            (
                best_forecast_params is null
                or best_forecast_params = 'null'
                or not contains(best_forecast_params::string, 'SEGMENTS')
            )
            and
            (ratio_params is null or ratio_params = 'null' or not contains(ratio_params::string, 'SEGMENTS'))
        )

),

-- Combine all records
combined as (

    select * from all_segments

    union all

    select * from no_segments

),

enriched as (

    select
        *,

        -- Calculate segment duration (only for records with segments)
        case
            when segment_start_date is not null and segment_end_date is not null
                then datediff('day', segment_start_date, segment_end_date)
        end as segment_duration_days,

        case
            when segment_start_date is not null and segment_end_date is not null
                then datediff('month', segment_start_date, segment_end_date)
        end as segment_duration_months,

        -- Decline type classification
        case
            when segment_type = 'arps' and b_factor = 0 then 'Exponential'
            when segment_type = 'arps' and b_factor > 0 and b_factor < 1 then 'Hyperbolic'
            when segment_type = 'arps' and b_factor = 1 then 'Harmonic'
            when segment_type = 'arps' and b_factor > 1 then 'Modified Hyperbolic'
            when segment_type = 'arps_modified' then 'Modified Hyperbolic'
            when segment_type = 'flat' then 'Flat Production'
            when segment_type = 'linear' then 'Linear Decline'
            when segment_type = 'ratio' then 'Ratio Forecast'
            when segment_type is null and forecast_type = 'ratio' then 'Ratio Forecast'
            else segment_type
        end as decline_type,

        -- Rate change metrics (only applicable to rate forecasts with q values)
        case
            when q_start is not null and q_end is not null
                then q_start - q_end
        end as rate_decline,

        case
            when q_start > 0 and q_end is not null
                then ((q_start - q_end) / q_start) * 100
        end as rate_decline_percentage,

        -- Annual effective decline rate (if using monthly nominal)
        case
            when decline_nominal > 0
                then
                    (1 - power(1 - decline_nominal, 12)) * 100
        end as annual_decline_percentage,

        -- Add flags for easier filtering
        coalesce(forecast_source in ('rate', 'rate_no_segments'), false) as is_rate_forecast,

        coalesce(forecast_source in ('ratio', 'ratio_no_segments'), false) as is_ratio_forecast,

        coalesce(segment_data is not null, false) as has_segments

    from combined

)

select * from enriched
