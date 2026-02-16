{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'project_scenarios') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as scenario_id,
        trim(project)::varchar as project_id,

        -- descriptive fields
        trim(name)::varchar as scenario_name,

        -- dates
        createdat::timestamp_ntz as created_at,
        updatedat::timestamp_ntz as updated_at,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where scenario_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['scenario_id']) }} as scenario_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        scenario_sk,
        scenario_id,
        project_id,
        scenario_name,
        created_at,
        updated_at,
        _portable_extracted,
        _loaded_at
    from enhanced
)

select * from final
