{{
    config(
        materialized='incremental',
        unique_key='tankmonthly_sk',
        tags=['griffin', 'marts', 'crescent']
    )
}}

/*
    Tank monthly fact for Griffin namespace.
    Monthly summary for storage tanks.
*/

with

monthly as (

    select * from {{ ref('stg_procount__tankmonthly') }}
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
        m.tankmonthly_sk,
        
        -- dimension keys
        t.tank_key,
        c.completion_key,
        
        -- natural keys
        m.merrick_id as tank_merrick_id,
        t.completion_merrick_id,
        m.record_date,
        m.start_date,
        m.end_date,

        -- tank context
        t.tank_name,
        t.tank_type,
        c.well_name,
        t.route_name,
        t.property_number,
        t.capacity as tank_capacity,

        -- hierarchy
        m.gathering_system_id,
        m.division_id,
        m.area_id,
        m.battery_id,
        m.tankbattery_id,
        m.mastertankbattery_id,
        m.group_id,
        m.platform_id,
        m.fieldgroup_id,

        -- business entities
        m.purchaserbe_id,
        m.purchaserbeloc,
        m.haulerbe_id,
        m.haulerbeloc,
        m.operatorentity_id,
        m.operatorentityloc,
        m.waterpurchaser as water_purchaser,
        m.waterhauler as water_hauler,
        m.purchasertank_id,
        m.haulertank_number,

        -- gauge readings - top open
        m.topopenfeet as top_open_feet,
        m.topopeninch as top_open_inch,
        m.topopenquarter as top_open_quarter,
        m.topopentotalinches as top_open_total_inches,

        -- gauge readings - top close
        m.topclosefeet as top_close_feet,
        m.topcloseinch as top_close_inch,
        m.topclosequarter as top_close_quarter,
        m.topclosetotalinches as top_close_total_inches,

        -- gauge readings - bottom open
        m.bottomopenfeet as bottom_open_feet,
        m.bottomopeninch as bottom_open_inch,
        m.bottomopenquarter as bottom_open_quarter,
        m.bottomopentotalinches as bottom_open_total_inches,

        -- gauge readings - bottom close
        m.bottomclosefeet as bottom_close_feet,
        m.bottomcloseinch as bottom_close_inch,
        m.bottomclosequarter as bottom_close_quarter,
        m.bottomclosetotalinches as bottom_close_total_inches,

        -- inventory - oil
        m.beginningoil as beginning_oil,
        m.endoil as ending_oil,

        -- inventory - water
        m.beginningwater as beginning_water,
        m.endwater as ending_water,

        -- inventory - NGL
        m.beginningngl as beginning_ngl,
        m.endingngl as ending_ngl,

        -- production
        m.oilproduction as oil_production,
        m.waterproduction as water_production,
        m.nglproduction as ngl_production,
        m.productionngl as production_ngl,

        -- run ticket totals
        m.totalrunsoil as total_runs_oil,
        m.totalrunswater as total_runs_water,
        m.totalrunsngl as total_runs_ngl,
        m.totalrunspumperentered as total_runs_pumper_entered,
        m.totalrunshaulerentered as total_runs_hauler_entered,

        -- hauler totals
        m.totalhauleroil as total_hauler_oil,
        m.totalhaulerwater as total_hauler_water,

        -- disposition totals
        m.totaldispositionoil as total_disposition_oil,
        m.totaldispositionwater as total_disposition_water,

        -- quality
        m.bsandw as bs_and_w,

        -- vapor
        m.tankvaporfactor as tank_vapor_factor,
        m.tankvaporvolume as tank_vapor_volume,
        m.gasequivalentfactor as gas_equivalent_factor,

        -- allocation
        m.allocationmethod as allocation_method,
        m.allocationorder as allocation_order,
        m.allocationbycomponent as allocation_by_component,
        m.productionallocdefault as production_alloc_default,
        m.dispositionallocdefault as disposition_alloc_default,
        m.measurementpointrole as measurement_point_role,
        m.tankbatteryrole as tank_battery_role,
        m.disposition_code,
        m.product_code,
        m.product_type,
        m.datasource_code,
        m.strapping_date,

        -- source flags
        m.tankmonthlyinvsource as tank_monthly_inv_source,
        m.tankcompbeginvsource as tank_comp_begin_v_source,
        m.tankdatasummary_code as tank_data_summary_code,

        -- calculated
        m.endoil - m.beginningoil as oil_inventory_change,
        m.endwater - m.beginningwater as water_inventory_change,

        -- teams/personnel
        m.productionteam_id,
        m.accountingteam_id,
        m.drillingteam_id,
        m.pumperperson_id,
        m.foremanperson_id,
        m.superperson_id,
        m.accountantperson_id,
        m.engineerperson_id,

        -- external IDs
        m.accounting_id,
        m.engineering_id,
        m.production_id,

        -- comments
        m.allocationcomment as allocation_comment,

        -- flags
        m.active_flag,
        m.backgroundtask_flag,
        m.calculationstatus_flag,
        m.tankdataentry_flag,
        m.loadtransfer_flag,
        m.firstday_flag,
        m.factorcomputation_flag,
        m.allownegativeinventory_flag,

        -- metadata
        m._fivetran_synced,
        m._loaded_at

    from monthly m
    left join tanks t
        on m.merrick_id = t.tank_merrick_id
    left join completions c
        on t.completion_merrick_id = c.merrick_id

)

select * from final
