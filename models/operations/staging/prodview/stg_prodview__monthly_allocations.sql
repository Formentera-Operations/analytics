{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTH') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent,
        trim(idflownet)::varchar as id_flownet,
        trim(idreccomp)::varchar as completion_id,
        trim(idreccomptk)::varchar as completion_table,
        trim(idreccompzone)::varchar as reporting_contact_interval_id,
        trim(idreccompzonetk)::varchar as reporting_contact_interval_table,

        -- dates
        dttmstart::timestamp_ntz as allocation_start_date,
        dttmend::timestamp_ntz as allocation_end_date,
        year::int as allocation_year,
        month::int as allocation_month,

        -- operational time
        {{ pv_days_to_hours('durdown') }}::float as downtime_hours,
        {{ pv_days_to_hours('durop') }}::float as operating_time_hours,

        -- gathered volumes
        {{ pv_cbm_to_bbl('volprodgathhcliq') }}::float as gathered_hcliq_bbl,
        {{ pv_cbm_to_mcf('volprodgathgas') }}::float as gathered_gas_mcf,
        {{ pv_cbm_to_bbl('volprodgathwater') }}::float as gathered_water_bbl,
        {{ pv_cbm_to_bbl('volprodgathsand') }}::float as gathered_sand_bbl,

        -- allocated volumes
        {{ pv_cbm_to_bbl('volprodallochcliq') }}::float as allocated_hcliq_bbl,
        {{ pv_cbm_to_bbl('volprodallocoil') }}::float as allocated_oil_bbl,
        {{ pv_cbm_to_bbl('volprodalloccond') }}::float as allocated_condensate_bbl,
        {{ pv_cbm_to_bbl('volprodallocngl') }}::float as allocated_ngl_bbl,
        {{ pv_cbm_to_mcf('volprodallochcliqgaseq') }}::float as allocated_hcliq_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volprodallocgas') }}::float as allocated_gas_mcf,
        {{ pv_cbm_to_bbl('volprodallocwater') }}::float as allocated_water_bbl,
        {{ pv_cbm_to_bbl('volprodallocsand') }}::float as allocated_sand_bbl,

        -- allocation factors
        allocfacthcliq::float as allocation_factor_hcliq,
        allocfactgas::float as allocation_factor_gas,
        allocfactwater::float as allocation_factor_water,
        allocfactsand::float as allocation_factor_sand,

        -- new production volumes
        {{ pv_cbm_to_bbl('volnewprodallochcliq') }}::float as new_production_hcliq_bbl,
        {{ pv_cbm_to_bbl('volnewprodallocoil') }}::float as new_production_oil_bbl,
        {{ pv_cbm_to_bbl('volnewprodalloccond') }}::float as new_production_condensate_bbl,
        {{ pv_cbm_to_bbl('volnewprodallocngl') }}::float as new_production_ngl_bbl,
        {{ pv_cbm_to_mcf('volnewprodallochcliqgaseq') }}::float as new_production_hcliq_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volnewprodallocgas') }}::float as new_production_gas_mcf,
        {{ pv_cbm_to_bbl('volnewprodallocwater') }}::float as new_production_water_bbl,
        {{ pv_cbm_to_bbl('volnewprodallocsand') }}::float as new_production_sand_bbl,

        -- working interest
        {{ pv_decimal_to_pct('wihcliq') }}::float as working_interest_oil_cond_pct,
        {{ pv_decimal_to_pct('wigas') }}::float as working_interest_gas_pct,
        {{ pv_decimal_to_pct('wiwater') }}::float as working_interest_water_pct,
        {{ pv_decimal_to_pct('wisand') }}::float as working_interest_sand_pct,

        -- net revenue interest
        {{ pv_decimal_to_pct('nrihcliq') }}::float as net_revenue_interest_oil_cond_pct,
        {{ pv_decimal_to_pct('nrigas') }}::float as net_revenue_interest_gas_pct,
        {{ pv_decimal_to_pct('nriwater') }}::float as net_revenue_interest_water_pct,
        {{ pv_decimal_to_pct('nrisand') }}::float as net_revenue_interest_sand_pct,

        -- lost production due to downtime
        {{ pv_cbm_to_bbl('vollosthcliq') }}::float as deferred_oil_condensate_production_bbl,
        {{ pv_cbm_to_mcf('vollostgas') }}::float as deferred_gas_production_mcf,
        {{ pv_cbm_to_bbl('vollostwater') }}::float as deferred_water_production_bbl,
        {{ pv_cbm_to_bbl('vollostsand') }}::float as deferred_sand_production_bbl,

        -- difference from target
        {{ pv_cbm_to_bbl('voldifftargethcliq') }}::float as difference_from_target_hcliq_bbl,
        {{ pv_cbm_to_bbl('voldifftargetoil') }}::float as difference_from_target_oil_bbl,
        {{ pv_cbm_to_bbl('voldifftargetcond') }}::float as difference_from_target_condensate_bbl,
        {{ pv_cbm_to_bbl('voldifftargetngl') }}::float as difference_from_target_ngl_bbl,
        {{ pv_cbm_to_mcf('voldifftargetgas') }}::float as difference_from_target_gas_mcf,
        {{ pv_cbm_to_bbl('voldifftargetwater') }}::float as difference_from_target_water_bbl,
        {{ pv_cbm_to_bbl('voldifftargetsand') }}::float as difference_from_target_sand_bbl,

        -- recoverable load/lift - starting volumes
        {{ pv_cbm_to_bbl('volstartremainrecovhcliq') }}::float as starting_load_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volstartremainrecovgas') }}::float as starting_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volstartremainrecovwater') }}::float as starting_load_water_bbl,
        {{ pv_cbm_to_bbl('volstartremainrecovsand') }}::float as starting_sand_bbl,

        -- recoverable load/lift - recovered volumes
        {{ pv_cbm_to_bbl('volrecovhcliq') }}::float as recovered_load_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volrecovgas') }}::float as recovered_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volrecovwater') }}::float as recovered_load_water_bbl,
        {{ pv_cbm_to_bbl('volrecovsand') }}::float as recovered_sand_bbl,

        -- recoverable load/lift - injected volumes
        {{ pv_cbm_to_mcf('volinjectrecovgas') }}::float as injected_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volinjectrecovhcliq') }}::float as injected_load_oil_condensate_bbl,
        {{ pv_cbm_to_bbl('volinjectrecovwater') }}::float as injected_load_water_bbl,
        {{ pv_cbm_to_bbl('volinjectrecovsand') }}::float as injected_sand_bbl,

        -- recoverable load/lift - remaining volumes
        {{ pv_cbm_to_bbl('volremainrecovhcliq') }}::float as remaining_load_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volremainrecovgas') }}::float as remaining_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volremainrecovwater') }}::float as remaining_load_water_bbl,
        {{ pv_cbm_to_bbl('volremainrecovsand') }}::float as remaining_sand_bbl,

        -- opening inventory
        {{ pv_cbm_to_bbl('volstartinvhcliq') }}::float as opening_inventory_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volstartinvhcliqgaseq') }}::float as opening_inventory_gas_equivalent_oil_cond_mcf,
        {{ pv_cbm_to_bbl('volstartinvwater') }}::float as opening_inventory_water_bbl,
        {{ pv_cbm_to_bbl('volstartinvsand') }}::float as opening_inventory_sand_bbl,

        -- closing inventory
        {{ pv_cbm_to_bbl('volendinvhcliq') }}::float as closing_inventory_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volendinvhcliqgaseq') }}::float as closing_inventory_gas_equiv_oil_condensate_mcf,
        {{ pv_cbm_to_bbl('volendinvwater') }}::float as closing_inventory_water_bbl,
        {{ pv_cbm_to_bbl('volendinvsand') }}::float as closing_inventory_sand_bbl,

        -- change in inventory
        {{ pv_cbm_to_bbl('volchginvhcliq') }}::float as change_in_inventory_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volchginvhcliqgaseq') }}::float as change_in_inventory_gas_equivalent_oil_cond_mcf,
        {{ pv_cbm_to_bbl('volchginvwater') }}::float as change_in_inventory_water_bbl,
        {{ pv_cbm_to_bbl('volchginvsand') }}::float as change_in_inventory_sand_bbl,

        -- dispositions - sales
        {{ pv_cbm_to_bbl('voldispsalehcliq') }}::float as disposed_allocated_sales_hcliq_bbl,
        {{ pv_cbm_to_bbl('voldispsaleoil') }}::float as disposed_allocated_sales_oil_bbl,
        {{ pv_cbm_to_bbl('voldispsalecond') }}::float as disposed_allocated_sales_condensate_bbl,
        {{ pv_cbm_to_bbl('voldispsalengl') }}::float as disposed_allocated_sales_ngl_bbl,
        {{ pv_cbm_to_mcf('voldispsalegas') }}::float as disposed_allocated_sales_gas_mcf,

        -- dispositions - gas uses
        {{ pv_cbm_to_mcf('voldispfuelgas') }}::float as disposed_allocated_fuel_gas_mcf,
        {{ pv_cbm_to_mcf('voldispflaregas') }}::float as disposed_allocated_flare_gas_mcf,
        {{ pv_cbm_to_mcf('voldispincinerategas') }}::float as disposed_allocated_incineration_gas_mcf,
        {{ pv_cbm_to_mcf('voldispventgas') }}::float as disposed_allocated_vent_gas_mcf,
        {{ pv_cbm_to_mcf('voldispinjectgas') }}::float as disposed_allocated_injected_gas_mcf,
        {{ pv_cbm_to_bbl('voldispinjectwater') }}::float as disposed_allocated_injected_water_bbl,

        -- injection well volumes
        {{ pv_cbm_to_bbl('volinjecthcliq') }}::float as injection_well_oil_cond_bbl,
        {{ pv_cbm_to_mcf('volinjectgas') }}::float as injection_well_gas_mcf,
        {{ pv_cbm_to_bbl('volinjectwater') }}::float as injection_well_water_bbl,
        {{ pv_cbm_to_bbl('volinjectsand') }}::float as injection_well_sand_bbl,

        -- cumulative production
        {{ pv_cbm_to_bbl('volprodcumhcliq') }}::float as cumulated_hcliq_bbl,
        {{ pv_cbm_to_bbl('volprodcumoil') }}::float as cumulated_oil_bbl,
        {{ pv_cbm_to_bbl('volprodcumcond') }}::float as cumulated_condensate_bbl,
        {{ pv_cbm_to_bbl('volprodcumngl') }}::float as cumulated_ngl_bbl,
        {{ pv_cbm_to_mcf('volprodcumgas') }}::float as cumulated_gas_mcf,
        {{ pv_cbm_to_bbl('volprodcumwater') }}::float as cumulated_water_bbl,
        {{ pv_cbm_to_bbl('volprodcumsand') }}::float as cumulated_sand_bbl,

        -- heat content
        {{ pv_joules_to_mmbtu('heatprodgath') }}::float as gathered_heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheatgath') }}::float as gathered_heat_factor_btu_per_ft3,
        {{ pv_joules_to_mmbtu('heatprodalloc') }}::float as allocated_heat_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheatalloc') }}::float as allocated_heat_factor_btu_per_ft3,
        {{ pv_joules_to_mmbtu('heatnewprodalloc') }}::float as new_production_heat_mmbtu,
        {{ pv_joules_to_mmbtu('heatdispsale') }}::float as disposed_sales_heat_mmbtu,
        {{ pv_joules_to_mmbtu('heatdispfuel') }}::float as disposed_fuel_heat_mmbtu,
        {{ pv_joules_to_mmbtu('heatdispflare') }}::float as disposed_flare_heat_mmbtu,
        {{ pv_joules_to_mmbtu('heatdispvent') }}::float as disposed_vent_heat_mmbtu,
        {{ pv_joules_to_mmbtu('heatdispincinerate') }}::float as disposed_incinerate_heat_mmbtu,

        -- density (API gravity - keep inline)
        (power(nullif(densityalloc, 0), -1) / 7.07409872233005E-06 + -131.5)::float as allocated_density_api,
        (power(nullif(densitysale, 0), -1) / 7.07409872233005E-06 + -131.5)::float as sales_density_api,

        -- reference IDs for related records
        trim(idrecmeasmeth)::varchar as last_measurement_method_id,
        trim(idrecmeasmethtk)::varchar as last_measurement_method_table,
        trim(idrecfluidlevel)::varchar as last_fluid_level_id,
        trim(idrecfluidleveltk)::varchar as last_fluid_level_table,
        trim(idrectest)::varchar as last_test_id,
        trim(idrectesttk)::varchar as last_test_table,
        trim(idrecparam)::varchar as last_param_id,
        trim(idrecparamtk)::varchar as last_param_table,
        trim(idrecdowntime)::varchar as downtime_id,
        trim(idrecdowntimetk)::varchar as downtime_table,
        trim(idrecdeferment)::varchar as deferment_id,
        trim(idrecdefermenttk)::varchar as deferment_table,
        trim(idrecgasanalysis)::varchar as gas_analysis_id,
        trim(idrecgasanalysistk)::varchar as gas_analysis_table,
        trim(idrechcliqanalysis)::varchar as hc_liquid_analysis_id,
        trim(idrechcliqanalysistk)::varchar as hc_liquid_analysis_table,
        trim(idrecoilanalysis)::varchar as oil_properties_id,
        trim(idrecoilanalysistk)::varchar as oil_properties_table,
        trim(idrecwateranalysis)::varchar as water_properties_id,
        trim(idrecwateranalysistk)::varchar as water_properties_table,
        trim(idrecstatus)::varchar as status_id,
        trim(idrecstatustk)::varchar as status_table,
        trim(idrecpumpentry)::varchar as last_pump_entry_id,
        trim(idrecpumpentrytk)::varchar as last_pump_entry_table,
        trim(idrecfacility)::varchar as reporting_facility_id,
        trim(idrecfacilitytk)::varchar as reporting_facility_table,
        trim(idreccalcset)::varchar as calc_settings_id,
        trim(idreccalcsettk)::varchar as calc_settings_table,

        -- other operational metrics
        {{ pv_decimal_to_pct('pumpeff') }}::float as pump_efficiency_pct,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,
        trim(systag)::varchar as record_tag,

        -- fivetran metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and id_rec is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as monthly_allocation_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        monthly_allocation_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        completion_id,
        completion_table,
        reporting_contact_interval_id,
        reporting_contact_interval_table,

        -- dates
        allocation_start_date,
        allocation_end_date,
        allocation_year,
        allocation_month,

        -- operational time
        downtime_hours,
        operating_time_hours,

        -- gathered volumes
        gathered_hcliq_bbl,
        gathered_gas_mcf,
        gathered_water_bbl,
        gathered_sand_bbl,

        -- allocated volumes
        allocated_hcliq_bbl,
        allocated_oil_bbl,
        allocated_condensate_bbl,
        allocated_ngl_bbl,
        allocated_hcliq_gas_equivalent_mcf,
        allocated_gas_mcf,
        allocated_water_bbl,
        allocated_sand_bbl,

        -- allocation factors
        allocation_factor_hcliq,
        allocation_factor_gas,
        allocation_factor_water,
        allocation_factor_sand,

        -- new production volumes
        new_production_hcliq_bbl,
        new_production_oil_bbl,
        new_production_condensate_bbl,
        new_production_ngl_bbl,
        new_production_hcliq_gas_equivalent_mcf,
        new_production_gas_mcf,
        new_production_water_bbl,
        new_production_sand_bbl,

        -- working interest
        working_interest_oil_cond_pct,
        working_interest_gas_pct,
        working_interest_water_pct,
        working_interest_sand_pct,

        -- net revenue interest
        net_revenue_interest_oil_cond_pct,
        net_revenue_interest_gas_pct,
        net_revenue_interest_water_pct,
        net_revenue_interest_sand_pct,

        -- lost production due to downtime
        deferred_oil_condensate_production_bbl,
        deferred_gas_production_mcf,
        deferred_water_production_bbl,
        deferred_sand_production_bbl,

        -- difference from target
        difference_from_target_hcliq_bbl,
        difference_from_target_oil_bbl,
        difference_from_target_condensate_bbl,
        difference_from_target_ngl_bbl,
        difference_from_target_gas_mcf,
        difference_from_target_water_bbl,
        difference_from_target_sand_bbl,

        -- recoverable load/lift - starting volumes
        starting_load_oil_condensate_bbl,
        starting_lift_gas_mcf,
        starting_load_water_bbl,
        starting_sand_bbl,

        -- recoverable load/lift - recovered volumes
        recovered_load_oil_condensate_bbl,
        recovered_lift_gas_mcf,
        recovered_load_water_bbl,
        recovered_sand_bbl,

        -- recoverable load/lift - injected volumes
        injected_lift_gas_mcf,
        injected_load_oil_condensate_bbl,
        injected_load_water_bbl,
        injected_sand_bbl,

        -- recoverable load/lift - remaining volumes
        remaining_load_oil_condensate_bbl,
        remaining_lift_gas_mcf,
        remaining_load_water_bbl,
        remaining_sand_bbl,

        -- opening inventory
        opening_inventory_oil_condensate_bbl,
        opening_inventory_gas_equivalent_oil_cond_mcf,
        opening_inventory_water_bbl,
        opening_inventory_sand_bbl,

        -- closing inventory
        closing_inventory_oil_condensate_bbl,
        closing_inventory_gas_equiv_oil_condensate_mcf,
        closing_inventory_water_bbl,
        closing_inventory_sand_bbl,

        -- change in inventory
        change_in_inventory_oil_condensate_bbl,
        change_in_inventory_gas_equivalent_oil_cond_mcf,
        change_in_inventory_water_bbl,
        change_in_inventory_sand_bbl,

        -- dispositions - sales
        disposed_allocated_sales_hcliq_bbl,
        disposed_allocated_sales_oil_bbl,
        disposed_allocated_sales_condensate_bbl,
        disposed_allocated_sales_ngl_bbl,
        disposed_allocated_sales_gas_mcf,

        -- dispositions - gas uses
        disposed_allocated_fuel_gas_mcf,
        disposed_allocated_flare_gas_mcf,
        disposed_allocated_incineration_gas_mcf,
        disposed_allocated_vent_gas_mcf,
        disposed_allocated_injected_gas_mcf,
        disposed_allocated_injected_water_bbl,

        -- injection well volumes
        injection_well_oil_cond_bbl,
        injection_well_gas_mcf,
        injection_well_water_bbl,
        injection_well_sand_bbl,

        -- cumulative production
        cumulated_hcliq_bbl,
        cumulated_oil_bbl,
        cumulated_condensate_bbl,
        cumulated_ngl_bbl,
        cumulated_gas_mcf,
        cumulated_water_bbl,
        cumulated_sand_bbl,

        -- heat content
        gathered_heat_mmbtu,
        gathered_heat_factor_btu_per_ft3,
        allocated_heat_mmbtu,
        allocated_heat_factor_btu_per_ft3,
        new_production_heat_mmbtu,
        disposed_sales_heat_mmbtu,
        disposed_fuel_heat_mmbtu,
        disposed_flare_heat_mmbtu,
        disposed_vent_heat_mmbtu,
        disposed_incinerate_heat_mmbtu,

        -- density
        allocated_density_api,
        sales_density_api,

        -- reference IDs for related records
        last_measurement_method_id,
        last_measurement_method_table,
        last_fluid_level_id,
        last_fluid_level_table,
        last_test_id,
        last_test_table,
        last_param_id,
        last_param_table,
        downtime_id,
        downtime_table,
        deferment_id,
        deferment_table,
        gas_analysis_id,
        gas_analysis_table,
        hc_liquid_analysis_id,
        hc_liquid_analysis_table,
        oil_properties_id,
        oil_properties_table,
        water_properties_id,
        water_properties_table,
        status_id,
        status_table,
        last_pump_entry_id,
        last_pump_entry_table,
        reporting_facility_id,
        reporting_facility_table,
        calc_settings_id,
        calc_settings_table,

        -- other operational metrics
        pump_efficiency_pct,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,
        record_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
