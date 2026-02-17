{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'general']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per reference well link)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVREFWELLS') }}
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

        -- reference well relationship
        trim(idrecrefwell)::varchar as reference_well_id,
        trim(idrecrefwelltk)::varchar as reference_well_table_key,
        trim(typ1)::varchar as relationship_type,
        trim(typ2)::varchar as relationship_subtype,
        trim(des)::varchar as relationship_description,

        -- dates
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,

        -- wellview data link
        trim(idrecitem)::varchar as wellview_data_link_id,
        trim(idrecitemtk)::varchar as wellview_data_link_table_key,

        -- distance (converted from meters to miles)
        {{ wv_meters_to_miles('dist') }} as distance_to_well_miles,

        -- comments
        trim(com)::varchar as comment,

        -- system / audit
        sysseq::float as sequence_number,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(syscreateuser)::varchar as created_by,
        sysmoddate::timestamp_ntz as last_mod_at_utc,
        trim(sysmoduser)::varchar as last_mod_by,
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
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as reference_well_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        reference_well_sk,

        -- identifiers
        record_id,
        well_id,

        -- reference well relationship
        reference_well_id,
        reference_well_table_key,
        relationship_type,
        relationship_subtype,
        relationship_description,

        -- dates
        start_date,
        end_date,

        -- wellview data link
        wellview_data_link_id,
        wellview_data_link_table_key,

        -- distance
        distance_to_well_miles,

        -- comments
        comment,

        -- system / audit
        sequence_number,
        created_at_utc,
        created_by,
        last_mod_at_utc,
        last_mod_by,
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
