{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'econ_runs') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as econ_run_id,
        trim(project)::varchar as project_id,
        trim(scenario)::varchar as scenario_id,

        -- descriptive fields
        trim(status)::varchar as status,

        -- dates
        rundate::timestamp_ntz as econ_run_date,

        -- variant / json fields
        tags,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where econ_run_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['econ_run_id']) }} as econ_run_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        econ_run_sk,
        econ_run_id,
        project_id,
        scenario_id,
        status,
        econ_run_date,
        tags,
        _portable_extracted,
        _loaded_at
    from enhanced
)

select * from final
