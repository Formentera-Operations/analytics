{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'zones_completions']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per zone record)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVZONE') }}
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
        trim(idrecwellbore)::varchar as wellbore_id,
        trim(idrecwellboretk)::varchar as wellbore_table_key,

        -- descriptive fields
        trim(zonename)::varchar as zone_name,
        trim(zonecode)::varchar as zone_code,
        trim(zoneida)::varchar as zone_api_number,
        trim(zoneidb)::varchar as zone_id_b,
        trim(zoneidc)::varchar as zone_id_c,
        trim(zoneidd)::varchar as zone_id_d,
        trim(zoneide)::varchar as zone_id_e,
        trim(objective)::varchar as objective,
        trim(formationcalc)::varchar as formation,
        trim(formationlayercalc)::varchar as formation_layer,
        trim(reservoircalc)::varchar as reservoir,
        trim(iconname)::varchar as icon_name,
        trim(currentstatuscalc)::varchar as current_status,
        trim(fieldname)::varchar as field_name,
        trim(fieldcode)::varchar as field_code,
        trim(unitname)::varchar as unit_name,
        trim(unitcode)::varchar as unit_code,
        trim(datasource)::varchar as data_source,

        -- depths (converted from metric to US units)
        {{ wv_meters_to_feet('depthtop') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtm') }} as bottom_depth_ft,
        {{ wv_meters_to_feet('depthref') }} as reference_depth_ft,
        {{ wv_meters_to_feet('depthtvdtopcalc') }} as top_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdbtmcalc') }} as bottom_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtvdrefcalc') }} as reference_depth_tvd_ft,
        {{ wv_meters_to_feet('depthtoptobtmcalc') }} as zone_thickness_ft,

        -- license
        trim(zonelicenseno)::varchar as zone_license_number,
        trim(zonelicensee)::varchar as zone_licensee,

        -- dates
        dttmzonelic::timestamp_ntz as zone_license_date,
        dttmzoneonprodest::timestamp_ntz as estimated_on_production_date,
        dttmzoneonprod::timestamp_ntz as first_production_date,
        dttmzonelastprodest::timestamp_ntz as estimated_last_production_date,
        dttmzonelastprod::timestamp_ntz as last_production_date,
        dttmzoneabandonest::timestamp_ntz as estimated_abandonment_date,
        dttmzoneabandon::timestamp_ntz as abandon_date,
        dttmstatuscalc::timestamp_ntz as current_status_date,

        -- completion reference
        trim(idreclastcompletioncalc)::varchar as last_completion_id,
        trim(idreclastcompletioncalctk)::varchar as last_completion_table_key,

        -- user fields
        trim(usertxt1)::varchar as user_text_1,
        trim(usertxt2)::varchar as user_text_2,
        trim(usertxt3)::varchar as user_text_3,
        trim(usertxt4)::varchar as user_text_4,
        trim(usertxt5)::varchar as user_text_5,
        trim(usertxt6)::varchar as user_text_6,

        -- comments
        trim(com)::varchar as comment,

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
        and record_id is not null
),

-- 4. ENHANCED: Add surrogate key + _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['record_id']) }} as zone_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        zone_sk,

        -- identifiers
        record_id,
        well_id,
        wellbore_id,
        wellbore_table_key,

        -- descriptive fields
        zone_name,
        zone_code,
        zone_api_number,
        zone_id_b,
        zone_id_c,
        zone_id_d,
        zone_id_e,
        objective,
        formation,
        formation_layer,
        reservoir,
        icon_name,
        current_status,
        field_name,
        field_code,
        unit_name,
        unit_code,
        data_source,

        -- depths
        top_depth_ft,
        bottom_depth_ft,
        reference_depth_ft,
        top_depth_tvd_ft,
        bottom_depth_tvd_ft,
        reference_depth_tvd_ft,
        zone_thickness_ft,

        -- license
        zone_license_number,
        zone_licensee,

        -- dates
        zone_license_date,
        estimated_on_production_date,
        first_production_date,
        estimated_last_production_date,
        last_production_date,
        estimated_abandonment_date,
        abandon_date,
        current_status_date,

        -- completion reference
        last_completion_id,
        last_completion_table_key,

        -- user fields
        user_text_1,
        user_text_2,
        user_text_3,
        user_text_4,
        user_text_5,
        user_text_6,

        -- comments
        comment,

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
