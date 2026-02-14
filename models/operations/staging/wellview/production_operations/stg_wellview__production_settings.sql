{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'production_operations']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per production setting period)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVPRODSETTING') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as production_setting_id,
        trim(idwell)::varchar as well_id,
        trim(idreczonecompletion)::varchar as zone_completion_id,
        trim(idreczonecompletiontk)::varchar as zone_completion_table_key,

        -- production method (canonical artificial lift type — sparsely populated)
        trim(prodmethtyp)::varchar as production_method_type,
        trim(prodmethdetail)::varchar as production_method_detail,

        -- setting details
        trim(settingobjective)::varchar as setting_objective,
        trim(settingresult)::varchar as setting_result,
        trim(com)::varchar as comment,

        -- pressures (converted to PSI)
        {{ wv_kpa_to_psi('prestub') }} as tubing_pressure_psi,
        {{ wv_kpa_to_psi('prescas') }} as casing_pressure_psi,

        -- dates
        dttmstart::timestamp_ntz as setting_start_date,
        dttmend::timestamp_ntz as setting_end_date,

        -- system locking
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,

        -- ingestion metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

-- 3. FILTERED: Remove soft deletes and null PKs. No transformations.
filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and production_setting_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['production_setting_id']) }} as production_setting_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        production_setting_sk,

        -- identifiers
        production_setting_id,
        well_id,
        zone_completion_id,
        zone_completion_table_key,

        -- production method
        production_method_type,
        production_method_detail,

        -- setting details
        setting_objective,
        setting_result,
        comment,

        -- pressures
        tubing_pressure_psi,
        casing_pressure_psi,

        -- dates
        setting_start_date,
        setting_end_date,

        -- system locking
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
        system_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
