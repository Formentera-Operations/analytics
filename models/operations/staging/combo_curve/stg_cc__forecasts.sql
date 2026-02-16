{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'forecasts') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as forecast_id,
        trim(project)::varchar as project_id,

        -- descriptive fields
        trim(name)::varchar as forecast_name,
        trim(type)::varchar as forecast_type,

        -- variant / json fields
        tags as forecast_tags,

        -- dates
        rundate::timestamp_ntz as run_date,
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where forecast_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['forecast_id']) }} as forecast_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        -- surrogate key
        forecast_sk,

        -- identifiers
        forecast_id,
        project_id,

        -- descriptive fields
        forecast_name,
        forecast_type,

        -- variant / json fields
        forecast_tags,

        -- dates
        run_date,
        created_at,
        updated_at,

        -- ingestion metadata
        _portable_extracted,

        -- dbt metadata
        _loaded_at

    from enhanced
)

select * from final
