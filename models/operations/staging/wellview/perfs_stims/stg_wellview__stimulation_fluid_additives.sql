{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per fluid additive record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTFLUIDADD') }}
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
        trim(typ1)::varchar as additive_type,
        trim(typ2)::varchar as additive_subtype,
        trim(des)::varchar as additive_name,
        trim(vendoraddname)::varchar as vendor_additive_name,
        trim(purpose)::varchar as purpose,
        refno::float as reference_number,
        trim(unitlabel)::varchar as units,
        trim(usertxt1)::varchar as user_text_1,
        trim(com)::varchar as comment,

        -- amount information
        amountdesign::float as design_amount,
        amount::float as actual_amount,

        -- additive properties
        density::float as additive_density_api,
        usernum1::float as user_number_1,

        -- calculated totals (converted to US units)
        {{ wv_kg_to_lb('masstotalcalc') }} as total_mass_of_additive_lb,
        {{ wv_cbm_to_bbl('voltotalcalc') }} as total_volume_of_additive_bbl,

        -- concentration limits (inline — percent, no macro)
        concmax / 0.01 as concentration_max_percent,
        concmin / 0.01 as concentration_min_percent,

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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as stimulation_fluid_additive_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        stimulation_fluid_additive_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,

        -- descriptive fields
        additive_type,
        additive_subtype,
        additive_name,
        vendor_additive_name,
        purpose,
        reference_number,
        units,
        user_text_1,
        comment,

        -- amount information
        design_amount,
        actual_amount,

        -- additive properties
        additive_density_api,
        user_number_1,

        -- calculated totals
        total_mass_of_additive_lb,
        total_volume_of_additive_bbl,

        -- concentration limits
        concentration_max_percent,
        concentration_min_percent,

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
