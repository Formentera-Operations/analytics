{{
  config(
    materialized='view'
  )
}}

-- Staging view for Combo Curve forecasted daily volumes
-- Transforms nested JSON structure into normalized rows
-- Note: This expands ~101k source rows to ~553M rows (3 phases × 1,827 days each)
-- Consider converting to table/incremental if performance becomes an issue

with source_data as (
  select
    _id as raw_id,
    _portable_extracted as extracted_at,
    forecast as forecast_id,
    forecasttype as forecast_type,
    phases as phases_variant,
    project as project_id,
    resolution,
    well as well_id
  from {{ source('combo_curve', 'forecasted_daily_volumes_by_project') }}
),

-- Parse the phases variant column
phases_parsed as (
  select
    sd.*,
    phase_value.value as phase_data,
    phase_value.value:phase::string as phase,
    phase_value.value:forecastOutputId::string as forecast_output_id
  from source_data sd,
    lateral flatten(input => parse_json(sd.phases_variant)) phase_value
),

-- Extract series data
series_extracted as (
  select
    pp.*,
    series_value.value as series_data,
    series_value.value:series::string as series_type,
    to_timestamp(series_value.value:startDate::string) as start_date,
    to_timestamp(series_value.value:endDate::string) as end_date,
    series_value.value:volumes as volumes_array
  from phases_parsed pp,
    lateral flatten(input => pp.phase_data:series) series_value
),

-- Unnest daily volumes with date calculation
daily_volumes as (
  select
    se.raw_id,
    se.extracted_at,
    se.forecast_id,
    se.forecast_type,
    se.project_id,
    se.resolution,
    se.well_id,
    se.phase,
    se.forecast_output_id,
    se.series_type,
    se.start_date,
    se.end_date,
    volume_value.index as day_index,
    dateadd(day, volume_value.index, se.start_date::date) as volume_date,
    volume_value.value::float as volume
  from series_extracted se,
    lateral flatten(input => se.volumes_array) volume_value
)

select
  -- IDs
  well_id,
  project_id,
  forecast_id,
  forecast_output_id,
  
  -- Forecast metadata
  forecast_type,
  series_type,
  resolution,
  
  -- Phase and volume data
  phase,
  volume_date,
  volume,
  
  -- Date range metadata
  start_date,
  end_date,
  day_index,
  
  -- Audit fields
  raw_id,
  extracted_at
from daily_volumes
where volume is not null  -- Filter out null volumes