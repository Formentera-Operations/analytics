{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per swab record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSWAB') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as swab_id,
        trim(idwell)::varchar as well_id,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idreczonecompletion)::varchar as completion_zone_id,
        trim(idreczonecompletiontk)::varchar as completion_zone_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual,
        trim(contractor)::varchar as swab_company,
        trim(com)::varchar as comments,

        -- volumes (converted to barrels)
        {{ wv_cbm_to_bbl('voltotalcalc') }} as total_volume_recovered_bbl,
        {{ wv_cbm_to_bbl('voltotaloilcalc') }} as total_oil_recovered_bbl,
        {{ wv_cbm_to_bbl('voltotalbswcalc') }} as total_bsw_recovered_bbl,

        -- gas volume (converted to MCF)
        {{ wv_cbm_to_mcf('voltotalgascalc') }} as total_gas_volume_mcf,

        -- dates
        dttm::timestamp_ntz as swab_date,

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
        and swab_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['swab_id']) }} as swab_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        swab_sk,

        -- identifiers
        swab_id,
        well_id,
        job_id,
        job_table_key,
        wellbore_id,
        wellbore_table_key,
        completion_zone_id,
        completion_zone_table_key,

        -- descriptive fields
        proposed_or_actual,
        swab_company,
        comments,

        -- volumes
        total_volume_recovered_bbl,
        total_oil_recovered_bbl,
        total_bsw_recovered_bbl,

        -- gas volume
        total_gas_volume_mcf,

        -- dates
        swab_date,

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
