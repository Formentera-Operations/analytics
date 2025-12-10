{{
    config(
        materialized='incremental',
        unique_key='meterdaily_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Meter daily fact for Griffin namespace.
    Daily readings and volumes for gas and liquid meters.
*/

with

daily as (

    select * from {{ ref('stg_procount__meterdaily') }}
    {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}

),

meters as (

    select
        meter_key,
        meter_merrick_id,
        completion_merrick_id,
        meter_name,
        meter_type as dim_meter_type,
        route_name,
        property_number,
        purchaserbe_id
    from {{ ref('dim_griffin__meters') }}

),

completions as (

    select
        completion_key,
        merrick_id,
        well_name
    from {{ ref('dim_griffin__completions') }}

),

final as (

    select
        -- surrogate key
        d.meterdaily_sk,
        
        -- dimension keys
        m.meter_key,
        c.completion_key,
        
        -- natural keys
        d.merrick_id as meter_merrick_id,
        m.completion_merrick_id,
        d.record_date,
        d.production_date,

        -- meter context
        m.meter_name,
        m.dim_meter_type,
        d.meter_type,
        c.well_name,
        m.route_name,
        m.property_number,
        m.purchaserbe_id,
        d.gathering_system_id,

        -- downtime
        d.downtime_flag,
        d.downtime_hours,

        -- allocated volumes
        d.allocoilvol as alloc_oil_vol,
        d.allocgas_vol_mcf,
        d.allocgas_vol_mmbtu,
        d.allocwatervol as alloc_water_vol,
        d.allocothervol as alloc_other_vol,
        d.allocco2vol as alloc_co2_vol,
        d.allocplantgasmcf as alloc_plant_gas_mcf,
        d.allocplantgasmmbtu as alloc_plant_gas_mmbtu,

        -- estimated volumes
        d.estoilvol as est_oil_vol,
        d.estgas_vol_mcf,
        d.estgas_vol_mmbtu,
        d.estwatervol as est_water_vol,
        d.estothervol as est_other_vol,
        d.estnglvol as est_ngl_vol,
        d.estco2vol as est_co2_vol,

        -- converted estimated volumes
        d.convestoilvol as convest_oil_vol,
        d.convestgas_vol_mcf,
        d.convestgas_vol_mmbtu,
        d.convestwatervol as convest_water_vol,
        d.convestothervol as convest_other_vol,
        d.convestco2vol as convest_co2_vol,

        -- fuel use volumes
        d.fueluseoilvol as fuel_use_oil_vol,
        d.fuelusegas_vol_mcf as fuel_use_gas_vol_mcf,
        d.fuelusegas_vol_mmbtu as fuel_use_gas_vol_mmbtu,
        d.fueluseothervol as fuel_use_other_vol,

        -- battery production
        d.batteryprodoil as battery_prod_oil,
        d.batteryprodgas as battery_prod_gas,
        d.batteryprodwater as battery_prod_water,
        d.batteryprodngl as battery_prod_ngl,
        d.allocoilprod as alloc_oil_prod,
        d.allocwaterprod as alloc_water_prod,

        -- run ticket volumes
        d.grossbarrels,
        d.netbarrels,
        d.bsandw as bs_and_w,

        -- gas nomination
        d.gasnominationmcf as gas_nomination_mcf,
        d.gasnominationmmbtu as gas_nomination_mmbtu,
        d.gasnominationbtufactor as gas_nomination_btu_factor,

        -- rates
        d.rateoil as rate_oil,
        d.rategas as rate_gas,
        d.ratewater as rate_water,
        d.rateco2 as rate_co2,

        -- pressures
        d.staticpressurepsia as static_pressure_psia,
        d.staticpressurepsig as static_pressure_psig,
        d.differentialpressure as differential_pressure,
        d.linepressure as line_pressure,
        d.absolutepressure,
        d.outputpressurepsia as output_pressure_psia,
        d.outputpressurepsig as output_pressure_psig,
        d.oilmetergaugepressure as oil_meter_gauge_pressure,
        d.estpressurebase as est_pressure_base,
        d.convestpressurebase as convest_pressure_base,
        d.allocpressurebase as alloc_pressure_base,
        d.pressureclassification as pressure_classification,

        -- pressure readings
        d.staticrange as static_range,
        d.staticpercentreading as static_percent_reading,
        d.staticrootreading as static_root_reading,
        d.differentialrange as differential_range,
        d.differentialpercentreading as differential_percent_reading,
        d.differentialrootreading as differential_root_reading,

        -- temperatures
        d.observedtemperature as observed_temperature,
        d.gastemperature as gas_temperature,
        d.opentemperature as open_temperature,
        d.closetemperature as close_temperature,
        d.esttemperature as est_temperature,
        d.convesttemperature as convest_temperature,
        d.alloctemperature as alloc_temperature,
        d.temperaturerangelow as temperature_range_low,
        d.temperaturerangehigh as temperature_range_high,

        -- quality factors
        d.estgravity as est_gravity,
        d.convestgravity as convest_gravity,
        d.allocgravity as alloc_gravity,
        d.actualgravity as actual_gravity,
        d.convertedgravity as converted_gravity,
        d.specificgravity as specific_gravity,
        d.estbtufactor as est_btu_factor,
        d.convestbtufactor as convest_btu_factor,
        d.allocbtufactor as alloc_btu_factor,
        d.shrinkagefactor as shrinkage_factor,
        d.gasequivalentfactor as gas_equivalent_factor,

        -- LACT factors
        d.lactmeterfactor as lact_meter_factor,
        d.lactcompressibilityfactor as lact_compressibility_factor,
        d.lactdensitycorrection as lact_density_correction,

        -- allocation factors
        d.allocationfactor as allocation_factor,
        d.allocationfactor_type as allocation_factor_type,
        d.allocationfactor2,
        d.allocationfactor3,
        d.allocationfactor4,
        d.allocationfactor5,
        d.allocationfactor6,
        d.allocationfactor7,
        d.allocationfactor8,
        d.meteradjustmentfactor as meter_adjustment_factor,
        d.mfactor as m_factor,

        -- proration factors
        d.prorationfactoroil as proration_factor_oil,
        d.prorationfactorgas as proration_factor_gas,
        d.prorationfactorwater as proration_factor_water,
        d.prorationfactorngl as proration_factor_ngl,

        -- equipment allocation factors
        d.equipallocfactor as equip_alloc_factor,
        d.equipallocfactor_type as equip_alloc_factor_type,
        d.equipallocfactor2,
        d.equipallocfactor3,
        d.equipallocfactor4,

        -- orifice/pipe specs
        d.orificeplatediameter as orifice_plate_diameter,
        d.pipetubediameter as pipe_tube_diameter,
        d.orificeholder_type,
        d.chokelength as choke_length,
        d.springsize as spring_size,

        -- odometer
        d.openodometer as open_odometer,
        d.closeodometer as close_odometer,

        -- hours
        d.hoursflowed as hours_flowed,
        d.readinghours as reading_hours,

        -- mass
        d.estmass as est_mass,
        d.estco2mass as est_co2_mass,
        d.allocco2mass as alloc_co2_mass,
        d.estsulfurmass as est_sulfur_mass,

        -- steam
        d.steamqualitypercent as steam_quality_percent,

        -- allocation settings
        d.allocationmethod as allocation_method,
        d.allocationmethodupstream as allocation_method_upstream,
        d.allocationorder as allocation_order,
        d.allocationbycomponent as allocation_by_component,
        d.allocatedvolumesource as allocated_volume_source,
        d.disposition_code,
        d.product_code,
        d.product_type,
        d.datasource_code,
        d.chart_type,
        d.chartcycle as chart_cycle,

        -- entered volume
        d.enteredvolume as entered_volume,
        d.enteredvolume_flag,

        -- flags
        d.backgroundtask_flag,
        d.transmit_flag,
        d.calculationstatus_flag,
        d.metercalculation_flag,
        d.volumeautopop_flag,
        d.volumebasis_flag,
        d.psi_flag,
        d.odometerreset_flag,
        d.units_flag,
        d.physicalmeter_flag,
        d.injectionmeaspoint_flag,
        d.batteryprodcalc_flag,
        d.loadtransfer_flag,
        d.setupdatachange_flag,
        d.ratecomputation_flag,
        d.factorcomputation_flag,
        d.computecoefficient_flag,
        d.prorateusingfueluse_flag,
        d.estwetdry_flag,
        d.convestwetdry_flag,
        d.allocwetdry_flag,
        d.fixedvolumeflagoil,
        d.fixedvolumeflaggas,
        d.fixedvolumeflagwater,
        d.flangepipe_flag,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from daily d
    left join meters m
        on d.merrick_id = m.meter_merrick_id
    left join completions c
        on m.completion_merrick_id = c.merrick_id

)

select * from final
