{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per tally record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVCASCOMPTALLY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as tally_id,
        trim(idrecparent)::varchar as component_id,
        trim(idwell)::varchar as well_id,
        sysseq::float as joint_sequence,

        -- joint identification
        jointrun::float as joint_run_number,
        runnocalc::float as calculated_run_number,
        trim(refid)::varchar as reference_id,
        trim(refno)::varchar as reference_number,
        trim(heatno)::varchar as heat_number,

        -- centralizer information
        trim(centralizersdes)::varchar as centralizer_description,
        centralizersno::float as centralizer_count,
        trim(extjewelry)::varchar as external_jewelry,

        -- centralizer flag raw value
        centralized::float as centralized_raw,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,

        -- lengths (converted from metric to US units)
        {{ wv_meters_to_feet('length') }} as joint_length_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumeinternalcumcalc') }} as cumulative_internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcumcalc') }} as cumulative_displaced_volume_bbl,

        -- weights (converted from metric to US units)
        {{ wv_newtons_to_klbf('weightcumcalc') }} as cumulative_weight_klbf,

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
        and tally_id is not null
),

-- 4. ENHANCED: Add surrogate key, computed flags, and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['tally_id']) }} as casing_tally_sk,
        *,
        coalesce(centralized_raw = 1, false) as has_centralizers,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        casing_tally_sk,

        -- identifiers
        tally_id,
        component_id,
        well_id,
        joint_sequence,

        -- joint identification
        joint_run_number,
        calculated_run_number,
        reference_id,
        reference_number,
        heat_number,

        -- centralizer information
        centralizer_description,
        centralizer_count,
        external_jewelry,

        -- depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,

        -- lengths
        joint_length_ft,
        cumulative_length_ft,

        -- volumes
        internal_volume_bbl,
        cumulative_internal_volume_bbl,
        cumulative_displaced_volume_bbl,

        -- weights
        cumulative_weight_klbf,

        -- flags
        has_centralizers,

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
