{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'general']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per status change event)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLSTATUSHISTORY') }}
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

        -- status details
        dttm::timestamp_ntz as status_date,
        trim(wellstatus1)::varchar as well_status,
        trim(wellstatus2)::varchar as well_sub_status,
        trim(welltyp1)::varchar as well_type,
        trim(welltyp2)::varchar as well_subtype,
        trim(primaryfluiddes)::varchar as primary_fluid_type,
        trim(source)::varchar as status_source,

        -- comments
        trim(com)::varchar as comment,

        -- system / audit
        syscreatedate::timestamp_ntz as created_at,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(sysmoduser)::varchar as modified_by,
        trim(systag)::varchar as system_tag,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockdate::timestamp_ntz as system_lock_date,

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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as well_status_history_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        well_status_history_sk,

        -- identifiers
        record_id,
        well_id,

        -- status details
        status_date,
        well_status,
        well_sub_status,
        well_type,
        well_subtype,
        primary_fluid_type,
        status_source,

        -- comments
        comment,

        -- system / audit
        created_at,
        created_by,
        modified_at,
        modified_by,
        system_tag,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_me,
        system_lock_children,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
