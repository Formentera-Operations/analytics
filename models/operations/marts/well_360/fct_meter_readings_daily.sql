{{
    config(
        materialized='incremental',
        unique_key='meter_reading_sk',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        cluster_by=['eid', 'reading_date'],
        tags=['marts', 'fo', 'well_360']
    )
}}

{#
    Fact: Meter Readings Daily
    ==============================

    PURPOSE:
    Daily raw measurement readings from ProdView's meter hierarchy, upstream
    of the allocation engine. Covers three meter types in a single UNION fact
    with a `meter_type` discriminator:

      - liquid     : pvUnitMeterLiquid (oil/condensate and water meters)
      - gas        : pvUnitMeterOrifice (gas orifice meters)
      - gas_pd     : pvUnitMeterPdGas (positive displacement gas meters)

    This is the raw gauge/reading layer. Allocation outputs from these meters
    flow into fct_well_production_daily (the allocation chain).

    GRAIN:
    One row per daily reading per meter installation per meter type.
    Natural key: (id_rec, meter_type). Surrogate key: MD5(id_rec || meter_type).

    EID RESOLUTION (2-hop — meters belong to pvUnit directly):
      reading.id_rec_parent → meter_header.id_rec
      meter_header.id_rec_parent → well_360.prodview_unit_id

    ROWS: ~7.4M (3M liquid + 4.1M gas + 236K gas_pd) as of Sprint 10.
    Incremental watermark: _fivetran_synced on each source staging model.

    METER-TYPE-SPECIFIC COLUMNS:
    - Liquid readings: hcliq_volume_bbl, water_volume_bbl, bsw_pct,
        uncorrected_volume_bbl, corrected_volume_bbl, sample_density_api
    - Gas readings: gas_volume_mcf, heat_mmbtu, heat_factor_btu_per_ft3,
        static_pressure_psi, differential_pressure_psi, c_prime_factor
    - Gas PD readings: gas_volume_mcf, heat_mmbtu, reading_value
    Columns not applicable to a meter type are NULL.

    DEPENDENCIES:
    - stg_prodview__liquid_meter_readings  + stg_prodview__liquid_meters
    - stg_prodview__gas_meter_readings     + stg_prodview__gas_meters
    - stg_prodview__gas_pd_meter_readings  + stg_prodview__gas_pd_meters
    - well_360
#}

with

-- =============================================================================
-- UNITS: bridge for api_10 fallback (meters belong to pvUnit directly)
-- =============================================================================
units as (
    select
        id_rec as unit_id_rec,
        api_10 as unit_api_10
    from {{ ref('stg_prodview__units') }}
),

-- =============================================================================
-- EID RESOLUTION LOOKUPS (shared across all meter types)
-- =============================================================================
well_dim_primary as (
    select
        prodview_unit_id as pvunit_id_rec,
        eid
    from {{ ref('well_360') }}
    where prodview_unit_id is not null
    qualify row_number() over (
        partition by prodview_unit_id
        order by case when is_operated then 0 else 1 end, eid
    ) = 1
),

well_dim_fallback as (
    select  -- noqa: ST06
        nullif(api_10, '') as pvunit_api_10,
        eid
    from {{ ref('well_360') }}
    where nullif(api_10, '') is not null
    qualify row_number() over (
        partition by nullif(api_10, '')
        order by case when is_operated then 0 else 1 end, eid
    ) = 1
),

-- =============================================================================
-- LIQUID METER READINGS (pvUnitMeterLiquid + pvUnitMeterLiquidEntry)
-- =============================================================================
liquid_with_unit as (
    select
        r.id_rec,
        'liquid' as meter_type,
        h.id_rec_parent as unit_id_rec,
        h.id_rec as meter_id_rec,
        h.meter_name,
        r.id_flownet,

        -- date
        r.reading_date,

        -- liquid-specific volumes
        r.total_volume_bbl as hcliq_volume_bbl,
        r.water_volume_bbl,
        r.uncorrected_total_volume_bbl as uncorrected_volume_bbl,
        r.corrected_total_volume_bbl as corrected_volume_bbl,
        r.final_bsw_pct as bsw_pct,
        r.sample_density_api,

        -- temperature / pressure
        r.volume_temperature_f as temperature_f,
        r.volume_pressure_psi as pressure_psi,

        -- gas meter specifics (NULL for liquid)
        null::float as gas_volume_mcf,
        null::float as heat_mmbtu,
        null::float as heat_factor_btu_per_ft3,
        null::float as static_pressure_psi,
        null::float as differential_pressure_psi,
        null::float as c_prime_factor,
        null::float as reading_value,

        -- quality flags
        r.is_verified,

        -- audit
        r.comments,
        r.created_at_utc,
        r.modified_at_utc,
        r._fivetran_synced

    from {{ ref('stg_prodview__liquid_meter_readings') }} r
    inner join {{ ref('stg_prodview__liquid_meters') }} h
        on r.id_rec_parent = h.id_rec
    {% if is_incremental() %}
        where r._fivetran_synced > (  -- noqa: LT14
            select coalesce(max(_fivetran_synced), '1900-01-01'::timestamp_tz)
            from {{ this }}
            where meter_type = 'liquid'
        )
    {% endif %}
),

-- =============================================================================
-- GAS METER READINGS (pvUnitMeterOrifice + pvUnitMeterOrificeEntry)
-- =============================================================================
gas_with_unit as (
    select
        r.id_rec,
        'gas' as meter_type,
        h.id_rec_parent as unit_id_rec,
        h.id_rec as meter_id_rec,
        h.meter_name,
        r.id_flownet,

        -- date
        r.reading_date,

        -- liquid specifics (NULL for gas)
        null::float as hcliq_volume_bbl,
        null::float as water_volume_bbl,
        null::float as uncorrected_volume_bbl,
        null::float as corrected_volume_bbl,
        null::float as bsw_pct,
        null::float as sample_density_api,

        -- temperature / pressure
        r.calculated_temperature_f as temperature_f,
        r.calculated_static_pressure_psi as pressure_psi,

        -- gas volumes and heat
        r.calculated_gas_volume_mcf as gas_volume_mcf,
        r.calculated_heat_mmbtu as heat_mmbtu,
        r.calculated_heat_factor_btu_per_ft3 as heat_factor_btu_per_ft3,
        r.calculated_static_pressure_psi as static_pressure_psi,
        r.calculated_differential_pressure_psi as differential_pressure_psi,
        r.c_prime_factor,
        null::float as reading_value,

        -- quality flags
        null::boolean as is_verified,

        -- audit
        r.comments,
        r.created_at_utc,
        r.modified_at_utc,
        r._fivetran_synced

    from {{ ref('stg_prodview__gas_meter_readings') }} r
    inner join {{ ref('stg_prodview__gas_meters') }} h
        on r.id_rec_parent = h.id_rec
    {% if is_incremental() %}
        where r._fivetran_synced > (  -- noqa: LT14
            select coalesce(max(_fivetran_synced), '1900-01-01'::timestamp_tz)
            from {{ this }}
            where meter_type = 'gas'
        )
    {% endif %}
),

-- =============================================================================
-- GAS PD METER READINGS (pvUnitMeterPdGas + pvUnitMeterPdGasEntry)
-- =============================================================================
gas_pd_with_unit as (
    select
        r.id_rec,
        'gas_pd' as meter_type,
        h.id_rec_parent as unit_id_rec,
        h.id_rec as meter_id_rec,
        h.meter_name,
        r.id_flownet,

        -- date
        r.reading_date,

        -- liquid specifics (NULL)
        null::float as hcliq_volume_bbl,
        null::float as water_volume_bbl,
        null::float as uncorrected_volume_bbl,
        null::float as corrected_volume_bbl,
        null::float as bsw_pct,
        null::float as sample_density_api,

        -- temperature / pressure
        r.temperature_f,
        r.pressure_psi,

        -- gas volumes and heat
        r.calculated_gas_volume_mcf as gas_volume_mcf,
        r.heat_mmbtu,
        r.heat_factor_btu_per_ft3,
        null::float as static_pressure_psi,
        null::float as differential_pressure_psi,
        null::float as c_prime_factor,
        r.reading_value,

        -- quality flags
        null::boolean as is_verified,

        -- audit (gas_pd uses 'note' instead of 'comments')
        r.note as comments,
        r.created_at_utc,
        r.modified_at_utc,
        r._fivetran_synced

    from {{ ref('stg_prodview__gas_pd_meter_readings') }} r
    inner join {{ ref('stg_prodview__gas_pd_meters') }} h
        on r.id_rec_parent = h.id_rec
    {% if is_incremental() %}
        where r._fivetran_synced > (  -- noqa: LT14
            select coalesce(max(_fivetran_synced), '1900-01-01'::timestamp_tz)
            from {{ this }}
            where meter_type = 'gas_pd'
        )
    {% endif %}
),

-- =============================================================================
-- UNION all meter types
-- =============================================================================
all_readings as (
    select * from liquid_with_unit
    union all
    select * from gas_with_unit
    union all
    select * from gas_pd_with_unit
),

-- =============================================================================
-- EID RESOLUTION: join unit_id → well_360
-- =============================================================================
readings_with_eid as (
    select  -- noqa: ST06
        coalesce(w1.eid, w2.eid) as eid,
        coalesce(w1.eid, w2.eid) is null as is_eid_unresolved,
        r.*

    from all_readings r
    left join units u
        on r.unit_id_rec = u.unit_id_rec
    left join well_dim_primary w1
        on r.unit_id_rec = w1.pvunit_id_rec
    left join well_dim_fallback w2
        on u.unit_api_10 = w2.pvunit_api_10
),

-- =============================================================================
-- FINAL ASSEMBLY
-- =============================================================================
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec', 'meter_type']) }}
            as meter_reading_sk,

        -- grain key
        id_rec,
        meter_type,

        -- FK bridges
        meter_id_rec,

        -- EID resolution
        eid,
        is_eid_unresolved,

        -- meter name
        meter_name,

        -- date
        reading_date,

        -- temperature / pressure (all types where applicable)
        temperature_f,
        pressure_psi,

        -- liquid meter columns (NULL for gas types)
        hcliq_volume_bbl,
        water_volume_bbl,
        uncorrected_volume_bbl,
        corrected_volume_bbl,
        bsw_pct,
        sample_density_api,
        is_verified,

        -- gas meter columns (NULL for liquid)
        gas_volume_mcf,
        heat_mmbtu,
        heat_factor_btu_per_ft3,
        static_pressure_psi,
        differential_pressure_psi,
        c_prime_factor,

        -- gas pd specific (NULL for liquid/gas)
        reading_value,

        -- audit
        comments,
        created_at_utc,
        modified_at_utc,
        _fivetran_synced,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from readings_with_eid
)

select * from final
