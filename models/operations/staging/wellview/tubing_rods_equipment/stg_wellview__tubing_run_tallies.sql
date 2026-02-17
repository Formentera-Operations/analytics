{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPTALLY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as tally_id,
        trim(idrecparent)::varchar as tubing_component_id,
        trim(idwell)::varchar as well_id,
        sysseq::int as sequence_number,

        -- descriptive fields
        jointrun::float as joint_run_number,
        runnocalc::float as run_number_calc,
        trim(centralizersdes)::varchar as centralizer_description,
        centralizersno::float as centralizer_number,
        trim(extjewelry)::varchar as external_jewelry,
        trim(refid)::varchar as reference_id,
        refno::float as reference_number,

        -- measurements — lengths (converted from meters to feet)
        {{ wv_meters_to_feet('length') }} as joint_length_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,

        -- measurements — depths (converted from meters to feet)
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,

        -- measurements — volumes (converted from cubic meters to barrels)
        {{ wv_cbm_to_bbl('volumeinternalcalc') }} as internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumeinternalcumcalc') }} as cumulative_internal_volume_bbl,
        {{ wv_cbm_to_bbl('volumedispcumcalc') }} as cumulative_displaced_volume_bbl,

        -- measurements — weight (converted from newtons to klbf)
        {{ wv_newtons_to_klbf('weightcumcalc') }} as cumulative_weight_klbf,

        -- flags
        centralized::boolean as is_centralized,

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

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and tally_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['tally_id']) }} as tally_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tally_sk,

        -- identifiers
        tally_id,
        tubing_component_id,
        well_id,
        sequence_number,

        -- descriptive fields
        joint_run_number,
        run_number_calc,
        centralizer_description,
        centralizer_number,
        external_jewelry,
        reference_id,
        reference_number,

        -- measurements — lengths
        joint_length_ft,
        cumulative_length_ft,

        -- measurements — depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,

        -- measurements — volumes
        internal_volume_bbl,
        cumulative_internal_volume_bbl,
        cumulative_displaced_volume_bbl,

        -- measurements — weight
        cumulative_weight_klbf,

        -- flags
        is_centralized,

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
