{{
    config(
        materialized='table',
        unique_key='completion_parameter_daily_sk',
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Completion Parameters Daily
    ==============================

    PURPOSE:
    Daily surveillance measurements per ProdView completion: pressures,
    temperatures, choke size, fluid properties, and plunger lift operations.
    Enables time-series pressure monitoring, plunger cycle tracking, and
    operational surveillance at the well level.

    No legacy equivalent — this is a new mart. The staging model
    (stg_prodview__completion_parameters) was previously only consumed by
    legacy wiserock_app views.

    GRAIN:
    One row per parameter measurement record (id_rec from PVT_PVUNITCOMPPARAM).
    Measurements are typically one per day per completion during active surveillance.
    measurement_date is a timestamp_ntz in staging; cast to date for grain clarity.

    EID RESOLUTION (three-hop via completions → unit → well_360):
    Same chain as fct_completion_downtime and fct_well_test:
    1. Primary:  parameter.id_rec_parent → completions.id_rec
                 → completions.id_rec_parent (unit ID)
                 → well_360.prodview_unit_id
    2. Fallback: completions.api_10 → well_360.api_10

    DEPENDENCIES:
    - stg_prodview__completion_parameters
    - stg_prodview__completions (for EID resolution)
    - well_360
#}

with

-- =============================================================================
-- COMPLETION PARAMETERS: all analytical columns from staging
-- =============================================================================
params as (
    select
        id_rec,
        id_rec_parent,   -- completion ID (FK to stg_prodview__completions.id_rec)
        measurement_date::date as measurement_date,

        -- pressure measurements
        tubing_pressure_psi,
        casing_pressure_psi,
        annulus_pressure_psi,
        line_pressure_psi,
        injection_pressure_psi,
        wellhead_pressure_psi,
        bottomhole_pressure_psi,
        shut_in_tubing_pressure_psi,
        shut_in_casing_pressure_psi,
        surface_casing_pressure_psi,
        intermediate_casing_pressure_psi,
        plunger_on_pressure_psi,
        annulus_pressure_2_psi,

        -- temperature measurements
        wellhead_temp_f,
        bottomhole_temp_f,
        treater_temp_f,

        -- equipment specifications
        choke_size_64ths,

        -- fluid properties
        h2s_daily_reading_ppm,
        ph_level,
        dynamic_viscosity_pa_s,

        -- plunger lift operations
        cycles,
        arrivals,
        travel_time_min,
        after_flow_min,
        shut_in_time_min,
        plunger_inspection_date,
        plunger_replace_date,
        plunger_model,
        plunger_make,
        plunger_size,

        -- operational notes
        notes,
        operational_work,
        spcc_inspection_complete,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced

    from {{ ref('stg_prodview__completion_parameters') }}
),

-- =============================================================================
-- COMPLETIONS: bridge for EID resolution keys
-- =============================================================================
completions as (
    select
        id_rec as completion_id_rec,
        id_rec_parent as unit_id_rec,
        api_10 as completion_api_10
    from {{ ref('stg_prodview__completions') }}
),

-- =============================================================================
-- EID RESOLUTION: deduplicated lookups from well_360
-- =============================================================================
well_dim_primary as (
    select
        prodview_unit_id as pvunit_id_rec,
        eid
    from {{ ref('well_360') }}
    where prodview_unit_id is not null
),

well_dim_fallback as (
    select
        api_10 as pvunit_api_10,
        eid
    from {{ ref('well_360') }}
    where api_10 is not null
    qualify row_number() over (
        partition by api_10
        order by
            case when is_operated then 0 else 1 end,
            eid
    ) = 1
),

-- =============================================================================
-- PARAMETERS WITH EID: join completions, resolve EID
-- =============================================================================
params_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,

        p.id_rec,
        p.id_rec_parent as id_rec_comp,
        p.measurement_date,
        p.tubing_pressure_psi,
        p.casing_pressure_psi,
        p.annulus_pressure_psi,
        p.line_pressure_psi,
        p.injection_pressure_psi,
        p.wellhead_pressure_psi,
        p.bottomhole_pressure_psi,
        p.shut_in_tubing_pressure_psi,
        p.shut_in_casing_pressure_psi,
        p.surface_casing_pressure_psi,
        p.intermediate_casing_pressure_psi,
        p.plunger_on_pressure_psi,
        p.annulus_pressure_2_psi,
        p.wellhead_temp_f,
        p.bottomhole_temp_f,
        p.treater_temp_f,
        p.choke_size_64ths,
        p.h2s_daily_reading_ppm,
        p.ph_level,
        p.dynamic_viscosity_pa_s,
        p.cycles,
        p.arrivals,
        p.travel_time_min,
        p.after_flow_min,
        p.shut_in_time_min,
        p.plunger_inspection_date,
        p.plunger_replace_date,
        p.plunger_model,
        p.plunger_make,
        p.plunger_size,
        p.notes,
        p.operational_work,
        p.spcc_inspection_complete,
        p.created_at_utc,
        p.modified_at_utc,
        p._fivetran_synced

    from params p
    left join completions c
        on p.id_rec_parent = c.completion_id_rec
    left join well_dim_primary w1
        on c.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on c.completion_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY: surrogate key
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }}
            as completion_parameter_daily_sk,

        -- grain keys
        id_rec,
        id_rec_comp,
        eid,
        is_eid_unresolved,
        measurement_date,

        -- pressure measurements
        tubing_pressure_psi,
        casing_pressure_psi,
        annulus_pressure_psi,
        line_pressure_psi,
        injection_pressure_psi,
        wellhead_pressure_psi,
        bottomhole_pressure_psi,
        shut_in_tubing_pressure_psi,
        shut_in_casing_pressure_psi,
        surface_casing_pressure_psi,
        intermediate_casing_pressure_psi,
        plunger_on_pressure_psi,
        annulus_pressure_2_psi,

        -- temperature measurements
        wellhead_temp_f,
        bottomhole_temp_f,
        treater_temp_f,

        -- equipment
        choke_size_64ths,

        -- fluid properties
        h2s_daily_reading_ppm,
        ph_level,
        dynamic_viscosity_pa_s,

        -- plunger lift operations
        cycles,
        arrivals,
        travel_time_min,
        after_flow_min,
        shut_in_time_min,
        plunger_inspection_date,
        plunger_replace_date,
        plunger_model,
        plunger_make,
        plunger_size,

        -- operational notes
        notes,
        operational_work,
        spcc_inspection_complete,

        -- audit
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from params_with_eid
)

select * from final
