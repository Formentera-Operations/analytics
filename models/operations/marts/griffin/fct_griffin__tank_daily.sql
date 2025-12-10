{{
    config(
        materialized='incremental',
        unique_key='tankdaily_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Tank daily fact for Griffin namespace.
    Daily gauge readings and inventory for storage tanks.
*/

with

daily as (

    select * from {{ ref('stg_procount__tankdaily') }}
    {% if is_incremental() %}
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
    {% endif %}

),

tanks as (

    select
        tank_key,
        tank_merrick_id,
        completion_merrick_id,
        tank_name,
        tank_type,
        route_name,
        property_number,
        capacity,
        barrelsperinch
    from {{ ref('dim_griffin__tanks') }}

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
        d.tankdaily_sk,
        
        -- dimension keys
        t.tank_key,
        c.completion_key,
        
        -- natural keys
        d.merrick_id as tank_merrick_id,
        t.completion_merrick_id,
        d.record_date,
        d.production_date,

        -- tank context
        t.tank_name,
        t.tank_type,
        c.well_name,
        t.route_name,
        t.property_number,
        t.capacity as tank_capacity,
        t.barrelsperinch as barrels_per_inch,

        -- gauge readings - top
        d.topfeet as top_feet,
        d.topinch as top_inch,
        d.topquarter as top_quarter,
        d.toptotalinches as top_total_inches,

        -- gauge readings - bottom (water)
        d.bottomfeet as bottom_feet,
        d.bottominch as bottom_inch,
        d.bottomquarter as bottom_quarter,
        d.bottomtotalinches as bottom_total_inches,

        -- inventory - oil
        d.beginningoil as beginning_oil,
        d.endingoil as ending_oil,
        d.adjustedbeginningoil as adjusted_beginning_oil,
        d.adjustedendingoil as adjusted_ending_oil,
        d.tankcompbeginningoil as tank_comp_beginning_oil,
        d.oilinventoryadjustment as oil_inventory_adjustment,

        -- inventory - water
        d.beginningwater as beginning_water,
        d.endingwater as ending_water,
        d.adjustedbeginningwater as adjusted_beginning_water,
        d.adjustedendingwater as adjusted_ending_water,
        d.tankcompbeginningwater as tank_comp_beginning_water,
        d.waterinventoryadjustment as water_inventory_adjustment,

        -- inventory - NGL
        d.beginningngl as beginning_ngl,
        d.endingngl as ending_ngl,

        -- production
        d.productionoil as production_oil,
        d.productionwater as production_water,
        d.productionngl as production_ngl,
        d.adjustedproductionoil as adjusted_production_oil,
        d.adjustedproductionwater as adjusted_production_water,
        d.othervolume as other_volume,

        -- run ticket totals
        d.totalrunsoil as total_runs_oil,
        d.grosstotalrunsoil as gross_total_runs_oil,
        d.totalrunswater as total_runs_water,
        d.totalrunsngl as total_runs_ngl,
        d.grossbarrelsoil as gross_barrels_oil,
        d.grossngl as gross_ngl,

        -- quality
        d.observedgravity as observed_gravity,
        d.convertedgravity as converted_gravity,
        d.bsandw as bs_and_w,
        d.watercut as water_cut,

        -- temperatures
        d.observedtemperature as observed_temperature,
        d.gaugetemperature as gauge_temperature,
        d.ambienttemperature as ambient_temperature,
        d.temperaturefactor as temperature_factor,

        -- pressure
        d.gaugepressure as gauge_pressure,

        -- vapor
        d.tankvaporfactor as tank_vapor_factor,
        d.tankvaporvolume as tank_vapor_volume,
        d.gasequivalentfactor as gas_equivalent_factor,

        -- rates
        d.rateoil as rate_oil,
        d.ratewater as rate_water,

        -- hours
        d.gaugehours as gauge_hours,
        d.tankgaugetime as tank_gauge_time,

        -- NGL percent
        d.percentfullngl as percent_full_ngl,

        -- allocation
        d.allocationmethod as allocation_method,
        d.allocationorder as allocation_order,
        d.allocationbycomponent as allocation_by_component,
        d.productionallocdefault as production_alloc_default,
        d.dispositionallocdefault as disposition_alloc_default,
        d.disposition_code,
        d.product_code,
        d.product_type,
        d.datasource_code,
        d.strapping_date,

        -- calculated
        d.endingoil - d.beginningoil as oil_inventory_change,
        d.endingwater - d.beginningwater as water_inventory_change,

        -- flags
        d.backgroundtask_flag,
        d.transmit_flag,
        d.calculationstatus_flag,
        d.tankdataentry_flag,
        d.tankisolated_flag,
        d.loadtransfer_flag,
        d.firstday_flag,
        d.factorcomputation_flag,
        d.error_number,

        -- metadata
        d._fivetran_synced,
        d._loaded_at

    from daily d
    left join tanks t
        on d.merrick_id = t.tank_merrick_id
    left join completions c
        on t.completion_merrick_id = c.merrick_id

)

select * from final
