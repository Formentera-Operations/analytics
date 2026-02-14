{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'perfs_stims']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per proppant record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVSTIMINTPROP') }}
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
        trim(typ1)::varchar as proppant_type,
        trim(typ2)::varchar as proppant_subtype,
        trim(des)::varchar as proppant_description,
        trim(sz)::varchar as sand_size,
        trim(note)::varchar as note,
        trim(usertxt1)::varchar as user_text_1,

        -- performance ratio
        ratiotamountdesigncalc::float as actual_to_design_proppant_mass_ratio,
        usernum1::float as user_number_1,

        -- proppant amounts (converted to pounds)
        {{ wv_kg_to_lb('amountdesign') }} as design_amount_lb,
        {{ wv_kg_to_lb('amount') }} as actual_amount_lb,
        {{ wv_kg_to_lb('amountcalc') }} as calculated_amount_lb,

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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as stimulation_proppant_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        stimulation_proppant_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,

        -- descriptive fields
        proppant_type,
        proppant_subtype,
        proppant_description,
        sand_size,
        note,
        user_text_1,

        -- performance ratio
        actual_to_design_proppant_mass_ratio,
        user_number_1,

        -- proppant amounts
        design_amount_lb,
        actual_amount_lb,
        calculated_amount_lb,

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
