{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'casing_cement']
    )
}}

with

-- 1. SOURCE: Raw data + Fivetran dedup on idrec (one row per wellhead component)
source as (
    select * from {{ source('wellview_calcs', 'WVT_WVWELLHEADCOMP') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

-- 2. RENAMED: Column renaming, type casting, trimming. No filtering, no logic.
renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as wellhead_component_id,
        trim(idwell)::varchar as well_id,
        trim(idrecparent)::varchar as wellhead_id,
        trim(idrecstring)::varchar as string_id,
        trim(idrecstringtk)::varchar as string_table_key,
        sysseq::float as sequence_number,

        -- component classification
        trim(typ1)::varchar as component_type,
        trim(typ2)::varchar as component_subtype,
        trim(des)::varchar as component_description,
        trim(sect)::varchar as component_section,

        -- manufacturer information
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as component_model,
        trim(sn)::varchar as serial_number,
        trim(material)::varchar as component_material,
        trim(refid)::varchar as reference_id,

        -- connection specifications
        trim(conntoptyp)::varchar as top_connection_type,
        trim(connbtmtyp)::varchar as bottom_connection_type,
        trim(service)::varchar as service_type,
        trim(productspeclevel)::varchar as product_specification_level,
        trim(packofftype)::varchar as packoff_type,
        trim(iconname)::varchar as icon_name,
        trim(usertxt)::varchar as user_text,
        trim(com)::varchar as comments,
        trim(idreclastfailurecalc)::varchar as last_failure_id,
        trim(idreclastfailurecalctk)::varchar as last_failure_table_key,

        -- sizes (converted from metric to US units)
        {{ wv_meters_to_inches('szid') }} as inner_diameter_in,
        {{ wv_meters_to_inches('szidnom') }} as nominal_inner_diameter_in,
        {{ wv_meters_to_inches('szodnom') }} as nominal_outer_diameter_in,
        {{ wv_meters_to_inches('minbore') }} as minimum_bore_in,
        {{ wv_meters_to_inches('conntopsz') }} as top_connection_size_in,
        {{ wv_meters_to_inches('connbtmsz') }} as bottom_connection_size_in,

        -- depths and lengths (converted from metric to US units)
        {{ wv_meters_to_feet('length') }} as component_length_ft,
        {{ wv_meters_to_feet('lengthcumcalc') }} as cumulative_length_ft,
        {{ wv_meters_to_feet('depthtopcalc') }} as top_depth_ft,
        {{ wv_meters_to_feet('depthbtmcalc') }} as bottom_depth_ft,

        -- pressures (converted from metric to US units)
        {{ wv_kpa_to_psi('workpres') }} as working_pressure_psi,
        {{ wv_kpa_to_psi('maxpres') }} as maximum_pressure_psi,
        {{ wv_kpa_to_psi('workprestop') }} as top_working_pressure_psi,
        {{ wv_kpa_to_psi('workpresbtm') }} as bottom_working_pressure_psi,

        -- temperatures (converted from metric to US units)
        {{ wv_celsius_to_fahrenheit('temprating') }} as temperature_rating_f,

        -- volumes (converted from metric to US units)
        {{ wv_cbm_to_bbl('volumevoid') }} as void_volume_bbl,

        -- cost
        cost::float as component_cost,
        trim(costunitlabel)::varchar as cost_unit_label,

        -- dates
        dttmstart::timestamp_ntz as installation_datetime,
        dttmend::timestamp_ntz as removal_datetime,
        dttmmanufacture::timestamp_ntz as manufacture_datetime,

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
        and wellhead_component_id is not null
),

-- 4. ENHANCED: Add surrogate key and _loaded_at.
enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['wellhead_component_id']) }} as wellhead_component_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

-- 5. FINAL: Explicit column list, logically grouped. This is the contract.
final as (
    select
        wellhead_component_sk,

        -- identifiers
        wellhead_component_id,
        well_id,
        wellhead_id,
        string_id,
        string_table_key,
        sequence_number,

        -- component classification
        component_type,
        component_subtype,
        component_description,
        component_section,

        -- manufacturer information
        manufacturer,
        component_model,
        serial_number,
        component_material,
        reference_id,

        -- connection specifications
        top_connection_type,
        bottom_connection_type,
        service_type,
        product_specification_level,
        packoff_type,
        icon_name,
        user_text,
        comments,
        last_failure_id,
        last_failure_table_key,

        -- sizes
        inner_diameter_in,
        nominal_inner_diameter_in,
        nominal_outer_diameter_in,
        minimum_bore_in,
        top_connection_size_in,
        bottom_connection_size_in,

        -- depths and lengths
        component_length_ft,
        cumulative_length_ft,
        top_depth_ft,
        bottom_depth_ft,

        -- pressures
        working_pressure_psi,
        maximum_pressure_psi,
        top_working_pressure_psi,
        bottom_working_pressure_psi,

        -- temperatures
        temperature_rating_f,

        -- volumes
        void_volume_bbl,

        -- cost
        component_cost,
        cost_unit_label,

        -- dates
        installation_datetime,
        removal_datetime,
        manufacture_datetime,

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
