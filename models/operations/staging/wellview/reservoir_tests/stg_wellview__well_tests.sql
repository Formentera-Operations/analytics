{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'reservoir_tests']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per well test)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLTESTTRANS') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as well_test_id,
        trim(idwell)::varchar as well_id,
        trim(idrecjob)::varchar as job_id,
        trim(idrecjobtk)::varchar as job_table_key,
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,
        trim(idreczonecompletion)::varchar as completion_zone_id,
        trim(idreczonecompletiontk)::varchar as completion_zone_table_key,

        -- descriptive fields
        trim(typ)::varchar as test_type,
        trim(subtyp)::varchar as test_subtype,
        trim(des)::varchar as test_description,
        trim(testedby)::varchar as tested_by,
        trim(producedto)::varchar as produced_to,
        trim(formationcalc)::varchar as formation,
        trim(formationlayercalc)::varchar as formation_layer,
        trim(reservoircalc)::varchar as reservoir,
        trim(phasesepmethod)::varchar as phase_separation_method,
        trim(surfacetestequip)::varchar as surface_test_equipment,
        trim(volumemethod)::varchar as volume_measurement_method,
        trim(porositysource)::varchar as porosity_source,
        trim(loadfluidtyp)::varchar as load_fluid_type,
        trim(com)::varchar as comments,

        -- measurements - depths (converted from metric to feet)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,

        -- measurements - porosity (converted to percentage)
        porosity / 0.01 as porosity_percent,

        -- measurements - load fluid volumes (converted to barrels)
        {{ wv_cbm_to_bbl('volloadfluid') }} as load_fluid_volume_bbl,
        {{ wv_cbm_to_bbl('volloadfluidunrecov') }} as load_fluid_unrecovered_bbl,
        {{ wv_cbm_to_bbl('volloadfluidrecovcalc') }} as load_fluid_recovered_bbl,
        volpercentloadfluidrecovcalc / 0.01 as load_fluid_recovery_percent,

        -- measurements - total production volumes (converted to field units)
        {{ wv_cbm_to_bbl('volumeoiltotalcalc') }} as total_oil_volume_bbl,
        {{ wv_cbm_to_bbl('volumecondtotalcalc') }} as total_condensate_volume_bbl,
        {{ wv_cbm_to_bbl('volumewatertotalcalc') }} as total_water_volume_bbl,
        {{ wv_cbm_to_mcf('volumegastotalcalc') }} as total_gas_volume_mcf,

        -- dates
        dttm::timestamp_ntz as test_date,

        -- flags
        displayflag::boolean as display_flag,

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
        and well_test_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['well_test_id']) }} as well_test_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        well_test_sk,

        -- identifiers
        well_test_id,
        well_id,
        job_id,
        job_table_key,
        wellbore_id,
        wellbore_table_key,
        completion_zone_id,
        completion_zone_table_key,

        -- descriptive fields
        test_type,
        test_subtype,
        test_description,
        tested_by,
        produced_to,
        formation,
        formation_layer,
        reservoir,
        phase_separation_method,
        surface_test_equipment,
        volume_measurement_method,
        porosity_source,
        load_fluid_type,
        comments,

        -- measurements - depths
        top_depth_ft,
        bottom_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,

        -- measurements - porosity
        porosity_percent,

        -- measurements - load fluid volumes
        load_fluid_volume_bbl,
        load_fluid_unrecovered_bbl,
        load_fluid_recovered_bbl,
        load_fluid_recovery_percent,

        -- measurements - total production volumes
        total_oil_volume_bbl,
        total_condensate_volume_bbl,
        total_water_volume_bbl,
        total_gas_volume_mcf,

        -- dates
        test_date,

        -- flags
        display_flag,

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
