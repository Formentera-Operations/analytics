{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per fluid system record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTFLUID') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as record_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as parent_record_id,

        -- descriptive fields
        trim(des)::varchar as fluid_description,
        trim(fluidname)::varchar as fluid_name,
        trim(vendorfluidname)::varchar as vendor_fluid_name,
        trim(typ1)::varchar as fluid_type_1,
        trim(typ2)::varchar as fluid_type_2,
        trim(purpose)::varchar as purpose,
        trim(vendor)::varchar as vendor,
        trim(vendorcode)::varchar as vendor_code,
        trim(source)::varchar as source_field,
        trim(environmenttyp)::varchar as environment_type,
        trim(evalmethod)::varchar as evaluation_method,
        trim(usertxt1)::varchar as user_text_1,
        trim(com)::varchar as comment,

        -- fluid properties
        fluiddensity::float as fluid_density_api,
        ph::float as ph_level,
        presvapor::float as vapor_pressure_kpa,
        ratiovolumedesigncalc::float as volume_design_ratio_bbl_per_bbl,
        usernum1::float as user_number_1,

        -- viscosity (converted to centipoise)
        {{ wv_pas_to_cp('viscosity') }} as viscosity_cp,

        -- temperature (converted to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('tempref') }} as reference_temperature_fahrenheit,

        -- filter size (converted to inches)
        {{ wv_meters_to_inches('filtersz') }} as filter_size_inches,

        -- mass (converted to pounds)
        {{ wv_kg_to_lb('masstotal') }} as mass_total_lb,

        -- volumes (converted to barrels)
        {{ wv_cbm_to_bbl('volume') }} as volume_bbl,
        {{ wv_cbm_to_bbl('volumecalc') }} as volume_calc_bbl,
        {{ wv_cbm_to_bbl('volumedesign') }} as volume_design_bbl,

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
        and record_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as stimulation_fluid_system_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        stimulation_fluid_system_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,

        -- descriptive fields
        fluid_description,
        fluid_name,
        vendor_fluid_name,
        fluid_type_1,
        fluid_type_2,
        purpose,
        vendor,
        vendor_code,
        source_field,
        environment_type,
        evaluation_method,
        user_text_1,
        comment,

        -- fluid properties
        fluid_density_api,
        ph_level,
        vapor_pressure_kpa,
        volume_design_ratio_bbl_per_bbl,
        user_number_1,

        -- measurements
        viscosity_cp,
        reference_temperature_fahrenheit,
        filter_size_inches,
        mass_total_lb,

        -- volumes
        volume_bbl,
        volume_calc_bbl,
        volume_design_bbl,

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
