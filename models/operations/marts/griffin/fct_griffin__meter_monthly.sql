{{
    config(
        materialized='incremental',
        unique_key='metermonthly_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Meter monthly fact for Griffin namespace.
    Monthly readings and volumes for gas and liquid meters.
*/

with

monthly as (

    select * from {{ ref('stg_procount__metermonthly') }}
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
        purchaserbe_id as dim_purchaserbe_id
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
        m.metermonthly_sk,
        
        -- dimension keys
        mt.meter_key,
        c.completion_key,
        
        -- natural keys
        m.merrick_id as meter_merrick_id,
        mt.completion_merrick_id,
        m.record_date,
        m.start_date,
        m.end_date,

        -- meter context
        mt.meter_name,
        mt.dim_meter_type,
        m.meter_type,
        c.well_name,
        mt.route_name,
        mt.property_number,

        -- hierarchy
        m.gathering_system_id,
        m.division_id,
        m.area_id,
        m.battery_id,
        m.group_id,
        m.platform_id,

        -- business entities
        m.purchaserbe_id,
        m.purchaserbeloc,
        m.gathererbe_id,
        m.gathererbeloc,
        m.gatherer_id,
        m.transporterbe_id,
        m.transporterbeloc,
        m.transporter_id,
        m.integratorbe_id,
        m.integratorbeloc,
        m.meteroperatorbe_id,
        m.meteroperatorbeloc,
        m.operator,
        m.purchasermeter_id,
        m.integratormeter_id,
        m.thirdpartymeter_id,

        -- actual volumes
        m.actoilvol as act_oil_vol,
        m.actgas_vol_mcf,
        m.actgas_vol_mmbtu,
        m.actwatervol as act_water_vol,
        m.actothervol as act_other_vol,
        m.actnglvol as act_ngl_vol,
        m.actco2vol as act_co2_vol,

        -- allocated volumes
        m.allocoilvol as alloc_oil_vol,
        m.allocgas_vol_mcf,
        m.allocgas_vol_mmbtu,
        m.allocwatervol as alloc_water_vol,
        m.allocothervol as alloc_other_vol,
        m.allocco2vol as alloc_co2_vol,
        m.allocplantgasmcf as alloc_plant_gas_mcf,
        m.allocplantgasmmbtu as alloc_plant_gas_mmbtu,
        m.allocoilprod as alloc_oil_prod,
        m.allocwaterprod as alloc_water_prod,

        -- daily allocated volumes
        m.dailyallocoilvol as daily_alloc_oil_vol,
        m.dailyallocgas_vol_mcf as daily_alloc_gas_vol_mcf,
        m.dailyallocgas_vol_mmbtu as daily_alloc_gas_vol_mmbtu,
        m.dailyallocwatervol as daily_alloc_water_vol,
        m.dailyallocothervol as daily_alloc_other_vol,
        m.dailyallocco2vol as daily_alloc_co2_vol,
        m.dailyallocplantgasmcf as daily_alloc_plant_gas_mcf,
        m.dailyallocplantgasmmbtu as daily_alloc_plant_gas_mmbtu,

        -- estimated volumes
        m.estoilvol as est_oil_vol,
        m.estgas_vol_mcf,
        m.estgas_vol_mmbtu,
        m.estwatervol as est_water_vol,
        m.estothervol as est_other_vol,
        m.estco2vol as est_co2_vol,

        -- converted actual volumes
        m.convactoilvol as convact_oil_vol,
        m.convactgas_vol_mcf,
        m.convactgas_vol_mmbtu,
        m.convactwatervol as convact_water_vol,
        m.convactothervol as convact_other_vol,
        m.convactco2vol as convact_co2_vol,

        -- fuel use volumes
        m.fueluseoilvol as fuel_use_oil_vol,
        m.fuelusegas_vol_mcf as fuel_use_gas_vol_mcf,
        m.fuelusegas_vol_mmbtu as fuel_use_gas_vol_mmbtu,
        m.fueluseothervol as fuel_use_other_vol,

        -- run ticket volumes
        m.grossbarrels,
        m.netbarrels,
        m.bsandw as bs_and_w,
        m.oilpipelinevol as oil_pipeline_vol,

        -- mass
        m.actmass as act_mass,
        m.actco2mass as act_co2_mass,
        m.actsulfurmass as act_sulfur_mass,
        m.estco2mass as est_co2_mass,
        m.allocco2mass as alloc_co2_mass,

        -- pressures
        m.actpressurebase as act_pressure_base,
        m.convactpressurebase as convact_pressure_base,
        m.allocpressurebase as alloc_pressure_base,
        m.estpressurebase as est_pressure_base,
        m.dailyallocpressurebase as daily_alloc_pressure_base,
        m.pressureclassification as pressure_classification,

        -- temperatures
        m.acttemperature as act_temperature,
        m.convacttemperature as convact_temperature,
        m.alloctemperature as alloc_temperature,
        m.esttemperature as est_temperature,
        m.dailyalloctemperature as daily_alloc_temperature,
        m.observedtemperature as observed_temperature,
        m.opentemperature as open_temperature,
        m.closetemperature as close_temperature,

        -- quality factors - gravity
        m.actgravity as act_gravity,
        m.convactgravity as convact_gravity,
        m.allocgravity as alloc_gravity,
        m.estgravity as est_gravity,
        m.dailyallocgravity as daily_alloc_gravity,
        m.actualgravity as actual_gravity,
        m.convertedgravity as converted_gravity,

        -- quality factors - BTU
        m.actbtufactor as act_btu_factor,
        m.convactbtufactor as convact_btu_factor,
        m.allocbtufactor as alloc_btu_factor,
        m.estbtufactor as est_btu_factor,
        m.dailyallocbtufactor as daily_alloc_btu_factor,
        m.btuvalue as btu_value,

        -- other factors
        m.shrinkagefactor as shrinkage_factor,
        m.gasequivalentfactor as gas_equivalent_factor,
        m.prorationfactor as proration_factor,
        m.errorcorrectionfactor as error_correction_factor,
        m.errorcorrectionfactordaily as error_correction_factor_daily,

        -- LACT factors
        m.lactmeterfactor as lact_meter_factor,
        m.lactcompressibilityfactor as lact_compressibility_factor,
        m.lactdensitycorrection as lact_density_correction,

        -- allocation factors
        m.allocationfactor as allocation_factor,
        m.allocationfactor_type as allocation_factor_type,
        m.allocationfactor2,
        m.allocationfactor3,
        m.allocationfactor4,
        m.allocationfactor5,
        m.allocationfactor6,
        m.allocationfactor7,
        m.allocationfactor8,
        m.meteradjustmentfactor as meter_adjustment_factor,
        m.linearcoefficient as linear_coefficient,

        -- equipment allocation factors
        m.equipallocfactor as equip_alloc_factor,
        m.equipallocfactor_type as equip_alloc_factor_type,
        m.equipallocfactor2,
        m.equipallocfactor3,
        m.equipallocfactor4,

        -- odometer
        m.openodometer as open_odometer,
        m.closeodometer as close_odometer,

        -- hours/days
        m.totalhoursflowed as total_hours_flowed,
        m.dayson as days_on,
        m.totaldowntimehours as total_downtime_hours,

        -- runs entered
        m.totalrunspumperentered as total_runs_pumper_entered,
        m.totalrunshaulerentered as total_runs_hauler_entered,

        -- orifice specs
        m.platesize as plate_size,
        m.springsize as spring_size,

        -- allocation settings
        m.allocationmethod as allocation_method,
        m.allocationmethodupstream as allocation_method_upstream,
        m.allocationorder as allocation_order,
        m.allocationbycomponent as allocation_by_component,
        m.allocatedvolumesource as allocated_volume_source,
        m.measurementpointrole as measurement_point_role,
        m.disposition_code,
        m.product_code,
        m.product_type,
        m.datasource_code,

        -- comments
        m.allocationcomment as allocation_comment,

        -- teams/personnel
        m.productionteam_id,
        m.accountingteam_id,
        m.drillingteam_id,
        m.pumperperson_id,
        m.foremanperson_id,
        m.superperson_id,
        m.accountantperson_id,
        m.checkmetermerrick_id,
        m.fieldgroup_id,

        -- flags
        m.active_flag,
        m.backgroundtask_flag,
        m.transmit_flag,
        m.calculationstatus_flag,
        m.summation_flag,
        m.monthlydatasource_flag,
        m.volumebasis_flag,
        m.units_flag,
        m.odometerreset_flag,
        m.physicalmeter_flag,
        m.injectionmeaspoint_flag,
        m.loadtransfer_flag,
        m.actualvolumeautopop_flag,
        m.equipmentfuelusage_flag,
        m.prorateusingfueluse_flag,
        m.factorcomputation_flag,
        m.allocationfactorauto_flag,
        m.equipallocfactorauto_flag,
        m.actwetdry_flag,
        m.convactwetdry_flag,
        m.estwetdry_flag,
        m.allocwetdry_flag,
        m.dailyallocwetdry_flag,
        m.fixedvolumeflagoil,
        m.fixedvolumeflaggas,
        m.fixedvolumeflagwater,

        -- metadata
        m._fivetran_synced,
        m._loaded_at

    from monthly m
    left join meters mt
        on m.merrick_id = mt.meter_merrick_id
    left join completions c
        on mt.completion_merrick_id = c.merrick_id

)

select * from final
