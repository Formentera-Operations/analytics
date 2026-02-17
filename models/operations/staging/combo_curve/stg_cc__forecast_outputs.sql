{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'forecast_outputs') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as forecast_output_id,
        trim(well)::varchar as well_id,
        trim(project)::varchar as project_id,
        trim(forecast)::varchar as forecast_id,
        trim(typecurve)::varchar as type_curve_id,

        -- forecast attributes
        trim(forecasttype)::varchar as forecast_type,
        trim(forecastsubtype)::varchar as forecast_subtype,
        trim(phase)::varchar as product_phase,
        trim(status)::varchar as forecast_status,
        trim(data_freq)::varchar as data_frequency,

        -- flags
        forecasted::boolean as is_forecasted,

        -- descriptive fields
        trim(forecastedby)::varchar as forecasted_by_user,
        trim(reviewedby)::varchar as reviewed_by_user,

        -- dates
        forecastedat::timestamp_ntz as forecasted_at,
        reviewedat::timestamp_ntz as reviewed_at,
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at,

        -- variant / json fields
        best as best_forecast_params,
        ratio as ratio_params,
        typecurveapplysettings as type_curve_apply_settings,
        typecurvedata as type_curve_data,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where forecast_output_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['forecast_output_id']) }} as forecast_output_sk,
        *,
        (forecast_status = 'approved') as is_approved,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        forecast_output_sk,

        -- identifiers
        forecast_output_id,
        well_id,
        project_id,
        forecast_id,
        type_curve_id,

        -- forecast attributes
        forecast_type,
        forecast_subtype,
        product_phase,
        forecast_status,
        data_frequency,

        -- flags
        is_forecasted,
        is_approved,

        -- descriptive fields
        forecasted_by_user,
        reviewed_by_user,

        -- dates
        forecasted_at,
        reviewed_at,
        created_at,
        updated_at,

        -- variant / json fields
        best_forecast_params,
        ratio_params,
        type_curve_apply_settings,
        type_curve_data,

        -- ingestion metadata
        _portable_extracted,

        -- dbt metadata
        _loaded_at

    from enhanced
)

select * from final
