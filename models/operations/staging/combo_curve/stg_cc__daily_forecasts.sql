{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'forecasted_daily_volumes_by_project') }}
    where project in (
        select project_id
        from {{ ref('corporate_reserve_scenarios') }}
    )
),

renamed as (
    select
        -- identifiers
        trim(_id)::varchar as raw_id,
        trim(well)::varchar as well_id,
        trim(project)::varchar as project_id,
        trim(forecast)::varchar as forecast_id,

        -- forecast attributes
        trim(forecasttype)::varchar as forecast_type,
        trim(resolution)::varchar as resolution,

        -- variant data
        phases as phases_variant,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where well_id is not null
),

-- Extra CTE: Parse phases from variant
phases_parsed as (
    select
        f.*,
        phase_value.value:phase::varchar as phase,
        phase_value.value:forecastOutputId::varchar as forecast_output_id,
        phase_value.value:series as series_array
    from filtered f,
        lateral flatten(input => parse_json(f.phases_variant)) phase_value
),

-- Extra CTE: Extract series data
series_extracted as (
    select
        pp.raw_id,
        pp.well_id,
        pp.project_id,
        pp.forecast_id,
        pp.forecast_type,
        pp.resolution,
        pp.phase,
        pp.forecast_output_id,
        pp._portable_extracted,
        series_value.value:series::varchar as series_type,
        to_timestamp(series_value.value:startDate::varchar) as series_start_date,
        to_timestamp(series_value.value:endDate::varchar) as series_end_date,
        series_value.value:volumes as volumes_array
    from phases_parsed pp,
        lateral flatten(input => pp.series_array) series_value
),

-- Extra CTE: Unnest daily volumes
daily_volumes as (
    select  -- noqa: ST06
        se.raw_id,
        se.well_id,
        se.project_id,
        se.forecast_id,
        se.forecast_type,
        se.resolution,
        se.phase,
        se.forecast_output_id,
        se.series_type,
        se.series_start_date,
        se.series_end_date,
        volume_value.index as day_index,
        dateadd(day, volume_value.index, se.series_start_date::date) as volume_date,
        volume_value.value::float as volume,
        se._portable_extracted
    from series_extracted se,
        lateral flatten(input => se.volumes_array) volume_value
    where volume_value.value is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'well_id',
            'forecast_id',
            'phase',
            'series_type',
            'volume_date',
        ]) }} as daily_forecast_sk,
        *,
        current_timestamp() as _loaded_at
    from daily_volumes
),

final as (
    select
        -- surrogate key
        daily_forecast_sk,

        -- identifiers
        well_id,
        project_id,
        forecast_id,
        forecast_output_id,

        -- forecast attributes
        forecast_type,
        series_type,
        resolution,
        phase,

        -- measures
        volume_date,
        volume,
        day_index,

        -- date range
        series_start_date,
        series_end_date,

        -- system / audit
        raw_id,

        -- ingestion metadata
        _portable_extracted,

        -- dbt metadata
        _loaded_at

    from enhanced
)

select * from final
