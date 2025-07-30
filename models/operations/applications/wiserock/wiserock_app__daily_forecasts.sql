{{
  config(
    materialized='incremental',
    unique_key=['well_id', 'project_id', 'forecast_id', 'volume_date'],
    on_schema_change='fail',
    cluster_by=['volume_date', 'well_id']
  )
}}

-- Incremental model that summarizes daily production across phases
-- Processes new forecast data incrementally based on extracted_at timestamp

with staged_volumes as (
  select * 
  from {{ ref('stg_cc__daily_forecasts') }}
  where forecast_id in (
        select forecast_id 
        from {{ ref('reserve_forecasts') }}
        where forecast_name = '2Q25 Reserves'
    )
  {% if is_incremental() %}
    -- Only process data extracted after the last run
    and extracted_at > (select max(extracted_at) from {{ this }})
  {% endif %}
),

-- Pivot phases into columns and calculate aggregates
daily_summary as (
  select
    well_id,
    project_id,
    forecast_id,
    volume_date,
    extracted_at,
    
    -- Individual phase volumes
    max(case when phase = 'gas' then volume end) as gas_volume,
    max(case when phase = 'oil' then volume end) as oil_volume,
    max(case when phase = 'water' then volume end) as water_volume,
    
    -- Get forecast metadata (same for all phases)
    max(start_date) as forecast_start_date,
    max(end_date) as forecast_end_date
    
  from staged_volumes
  group by 1, 2, 3, 4, 5
),

-- Add calculated fields
final as (
  select
    -- Primary keys
    well_id,
    project_id,
    forecast_id,
    volume_date,
    
    -- Production volumes
    coalesce(gas_volume, 0) as gas_volume,
    coalesce(oil_volume, 0) as oil_volume,
    coalesce(water_volume, 0) as water_volume,
    
    
    -- BOE (Barrel of Oil Equivalent) - 6 MCF gas = 1 BOE
    coalesce(oil_volume, 0) + (coalesce(gas_volume, 0) / 6) as boe_volume,
    
    -- Gas-Oil Ratio (GOR) - MCF gas per barrel of oil
    case 
      when coalesce(oil_volume, 0) > 0 
      then coalesce(gas_volume, 0) / oil_volume
      else null 
    end as gas_oil_ratio,
    
    -- Water Cut - percentage of water in total liquids
    case 
      when (coalesce(oil_volume, 0) + coalesce(water_volume, 0)) > 0
      then coalesce(water_volume, 0) / (oil_volume + water_volume) * 100
      else null
    end as water_cut_pct,
    
    -- Time-based fields
    extract(year from volume_date) as production_year,
    extract(month from volume_date) as production_month,
    extract(quarter from volume_date) as production_quarter,
    
    -- Forecast metadata
    forecast_start_date,
    forecast_end_date,
    datediff(day, forecast_start_date, volume_date) as days_from_forecast_start,
    
    -- Audit fields
    extracted_at,
    current_timestamp() as dbt_inserted_at,
    current_timestamp() as dbt_updated_at
    
  from daily_summary
)

select * from final