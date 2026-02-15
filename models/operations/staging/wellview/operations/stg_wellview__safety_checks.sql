{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per safety check record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBSAFETYCHK') }}
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
        trim(typ)::varchar as check_type,
        trim(typfrequency)::varchar as check_frequency,
        trim(result)::varchar as check_result,
        trim(inspector)::varchar as inspector,
        trim(tour)::varchar as tour,
        trim(des)::varchar as description,
        trim(com)::varchar as comment,

        -- temporal
        dttm::timestamp_ntz as check_datetime,
        nextdttmcalc::timestamp_ntz as next_check_datetime,
        reportnocalc::float as report_number,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,

        -- system locking
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,
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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as safety_check_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        safety_check_sk,

        -- identifiers
        record_id,
        well_id,
        parent_record_id,

        -- descriptive fields
        check_type,
        check_frequency,
        check_result,
        inspector,
        tour,
        description,
        comment,

        -- temporal
        check_datetime,
        next_check_datetime,
        report_number,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,

        -- system locking
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,
        system_lock_date,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
