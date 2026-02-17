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
    where
        forecast_id in (
            select forecast_id
            from {{ ref('reserve_forecasts') }}
            where forecast_name = '2Q25 Reserves'
        )
    {% if is_incremental() %}
    -- Only process data extracted after the last run
    and _portable_extracted > (select max(_portable_extracted) from {{ this }})
  {% endif %}
),

-- Pivot phases into columns and calculate aggregates
daily_summary as (
    select
        well_id,
        project_id,
        forecast_id,
        volume_date,
        _portable_extracted,

        -- Individual phase volumes
        max(case when phase = 'gas' then volume end) as gas_volume,
        max(case when phase = 'oil' then volume end) as oil_volume,
        max(case when phase = 'water' then volume end) as water_volume,

        -- Get forecast metadata (same for all phases)
        max(series_start_date) as forecast_start_date,
        max(series_end_date) as forecast_end_date

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
        forecast_start_date,
        forecast_end_date,
        _portable_extracted,


        -- BOE (Barrel of Oil Equivalent) - 6 MCF gas = 1 BOE
        coalesce(gas_volume, 0) as gas_volume,

        -- Gas-Oil Ratio (GOR) - MCF gas per barrel of oil
        coalesce(oil_volume, 0) as oil_volume,

        -- Water Cut - percentage of water in total liquids
        coalesce(water_volume, 0) as water_volume,

        -- Time-based fields
        coalesce(oil_volume, 0) + (coalesce(gas_volume, 0) / 6) as boe_volume,
        case
            when coalesce(oil_volume, 0) > 0
                then coalesce(gas_volume, 0) / oil_volume
        end as gas_oil_ratio,
        case
            when (coalesce(oil_volume, 0) + coalesce(water_volume, 0)) > 0
                then coalesce(water_volume, 0) / (oil_volume + water_volume) * 100
        end as water_cut_pct,

        -- Forecast metadata
        extract(year from volume_date) as production_year,
        extract(month from volume_date) as production_month,
        extract(quarter from volume_date) as production_quarter,

        -- Audit fields
        datediff(day, forecast_start_date, volume_date) as days_from_forecast_start,
        current_timestamp() as dbt_inserted_at,
        current_timestamp() as dbt_updated_at

    from daily_summary
)

select * from final
