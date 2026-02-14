{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per wellhead)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEAD') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as wellhead_id,
        trim(idwell)::varchar as well_id,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,
        trim(idrecstring)::varchar as annulus_string_id,
        trim(idrecstringtk)::varchar as annulus_string_table_key,
        trim(idrecjobprogramphasecalc)::varchar as program_phase_id,
        trim(idrecjobprogramphasecalctk)::varchar as program_phase_table_key,
        trim(idreclastrigcalc)::varchar as last_rig_id,
        trim(idreclastrigcalctk)::varchar as last_rig_table_key,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- descriptive fields
        trim(proposedoractual)::varchar as proposed_or_actual,
        proprunversionno::float as proposed_run_version_number,
        trim(typ)::varchar as wellhead_type,
        trim(make)::varchar as manufacturer,
        trim(profile)::varchar as wellhead_profile,
        trim(service)::varchar as service_type,
        trim(class)::varchar as wellhead_class,
        trim(tempratingdes)::varchar as temperature_rating_description,
        trim(productspeclevel)::varchar as product_specification_level,
        trim(removereason)::varchar as removal_reason,
        trim(com)::varchar as comments,

        -- sizes (converted from metric to US units)
        {{ wv_meters_to_inches('sz') }} as wellhead_size_in,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthbtm') }} as set_depth_ft,

        -- pressures (converted from metric to US units)
        {{ wv_kpa_to_psi('workpres') }} as working_pressure_psi,
        {{ wv_kpa_to_psi('maxpres') }} as maximum_pressure_psi,

        -- temperatures (converted from metric to US units)
        {{ wv_celsius_to_fahrenheit('temprating') }} as temperature_rating_f,

        -- dates
        dttmstart::timestamp_ntz as installation_datetime,
        dttmend::timestamp_ntz as removal_datetime,
        dttmoverhaul::timestamp_ntz as overhaul_datetime,
        dttmpropend::timestamp_ntz as proposed_removal_datetime,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

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
        and wellhead_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['wellhead_id']) }} as wellhead_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        wellhead_sk,

        -- identifiers
        wellhead_id,
        well_id,
        job_id,
        job_table_key,
        annulus_string_id,
        annulus_string_table_key,
        program_phase_id,
        program_phase_table_key,
        last_rig_id,
        last_rig_table_key,
        last_failure_id,
        last_failure_table_key,

        -- descriptive fields
        proposed_or_actual,
        proposed_run_version_number,
        wellhead_type,
        manufacturer,
        wellhead_profile,
        service_type,
        wellhead_class,
        temperature_rating_description,
        product_specification_level,
        removal_reason,
        comments,

        -- sizes
        wellhead_size_in,

        -- depths
        set_depth_ft,

        -- pressures
        working_pressure_psi,
        maximum_pressure_psi,

        -- temperatures
        temperature_rating_f,

        -- dates
        installation_datetime,
        removal_datetime,
        overhaul_datetime,
        proposed_removal_datetime,

        -- system / audit
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
