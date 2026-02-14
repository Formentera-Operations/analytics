{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'tubing_rods_equipment']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVTUBCOMPMANDRELINSERT') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as insert_id,
        trim(idrecparent)::varchar as mandrel_id,
        trim(idwell)::varchar as well_id,

        -- descriptive fields
        trim(valvetyp)::varchar as valve_type,
        trim(valvedes)::varchar as valve_description,
        trim(valvematerial)::varchar as valve_material,
        trim(valvepacking)::varchar as valve_packing,
        trim(make)::varchar as manufacturer,
        trim(model)::varchar as model,
        trim(sn)::varchar as serial_number,
        trim(refid)::varchar as reference_id,
        trim(latchtyp)::varchar as latch_type,
        trim(latchmaterial)::varchar as latch_material,
        trim(orificematerial)::varchar as orifice_material,
        trim(retrievemeth)::varchar as retrieval_method,
        trim(pullreason)::varchar as pull_reason,
        trim(service)::varchar as service_type,
        trim(com)::varchar as comments,

        -- measurements — dimensions (converted from meters to inches)
        {{ wv_meters_to_inches('szod') }} as od_inches,
        {{ wv_meters_to_inches('szport') }} as port_size_inches,

        -- measurements — pressure (converted from kPa to psi)
        {{ wv_kpa_to_psi('trorun') }} as tro_run_psi,
        {{ wv_kpa_to_psi('tropull') }} as tro_pull_psi,
        {{ wv_kpa_to_psi('pressurfgaugeopen') }} as surface_gauge_pressure_open_psi,
        {{ wv_kpa_to_psi('pressurfgaugeclose') }} as surface_gauge_pressure_close_psi,

        -- measurements — temperature (converted from Celsius to Fahrenheit)
        {{ wv_celsius_to_fahrenheit('temp') }} as temperature_fahrenheit,

        -- dates
        dttmrun::timestamp_ntz as run_date,
        dttmpull::timestamp_ntz as pull_date,

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
        and insert_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['insert_id']) }} as mandrel_insert_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        mandrel_insert_sk,

        -- identifiers
        insert_id,
        mandrel_id,
        well_id,

        -- descriptive fields
        valve_type,
        valve_description,
        valve_material,
        valve_packing,
        manufacturer,
        model,
        serial_number,
        reference_id,
        latch_type,
        latch_material,
        orifice_material,
        retrieval_method,
        pull_reason,
        service_type,
        comments,

        -- measurements — dimensions
        od_inches,
        port_size_inches,

        -- measurements — pressure
        tro_run_psi,
        tro_pull_psi,
        surface_gauge_pressure_open_psi,
        surface_gauge_pressure_close_psi,

        -- measurements — temperature
        temperature_fahrenheit,

        -- dates
        run_date,
        pull_date,

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
