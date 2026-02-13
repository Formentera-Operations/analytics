{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
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
        trim(idrecunit)::varchar as id_rec_unit,
        trim(idrecunittk)::varchar as id_rec_unit_tk,
        trim(idreccomp)::varchar as id_rec_comp,
        trim(idreccomptk)::varchar as id_rec_comp_tk,
        trim(idreccompzone)::varchar as id_rec_comp_zone,
        trim(idreccompzonetk)::varchar as id_rec_comp_zone_tk,

        -- date/time
        dttm::timestamp_ntz as allocation_date,
        year::int as allocation_year,
        month::int as allocation_month,
        dayofmonth::int as allocation_day_of_month,

        -- operational time (days → hours)
        ({{ pv_days_to_hours('durdown') }})::float as downtime_hours,
        ({{ pv_days_to_hours('durop') }})::float as operating_time_hours,

        -- gathered volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volprodgathhcliq') }})::float as gathered_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volprodgathgas') }})::float as gathered_gas_mcf,
        ({{ pv_cbm_to_bbl('volprodgathwater') }})::float as gathered_water_bbl,
        ({{ pv_cbm_to_bbl('volprodgathsand') }})::float as gathered_sand_bbl,

        -- allocated volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volprodallochcliq') }})::float as allocated_hcliq_bbl,
        ({{ pv_cbm_to_bbl('volprodallocoil') }})::float as allocated_oil_bbl,
        ({{ pv_cbm_to_bbl('volprodalloccond') }})::float as allocated_condensate_bbl,
        ({{ pv_cbm_to_bbl('volprodallocngl') }})::float as allocated_ngl_bbl,
        ({{ pv_cbm_to_mcf('volprodallochcliqgaseq') }})::float as allocated_gas_eq_hcliq_mcf,
        ({{ pv_cbm_to_mcf('volprodallocgas') }})::float as allocated_gas_mcf,
        ({{ pv_cbm_to_bbl('volprodallocwater') }})::float as allocated_water_bbl,
        ({{ pv_cbm_to_bbl('volprodallocsand') }})::float as allocated_sand_bbl,

        -- allocation factors (unitless)
        allocfacthcliq::float as alloc_factor_hcliq,
        allocfactgas::float as alloc_factor_gas,
        allocfactwater::float as alloc_factor_water,
        allocfactsand::float as alloc_factor_sand,

        -- new production volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volnewprodallochcliq') }})::float as new_prod_hcliq_bbl,
        ({{ pv_cbm_to_bbl('volnewprodallocoil') }})::float as new_prod_oil_bbl,
        ({{ pv_cbm_to_bbl('volnewprodalloccond') }})::float as new_prod_condensate_bbl,
        ({{ pv_cbm_to_bbl('volnewprodallocngl') }})::float as new_prod_ngl_bbl,
        ({{ pv_cbm_to_mcf('volnewprodallochcliqgaseq') }})::float as new_prod_hcliq_gas_eq_mcf,
        ({{ pv_cbm_to_mcf('volnewprodallocgas') }})::float as new_prod_gas_mcf,
        ({{ pv_cbm_to_bbl('volnewprodallocwater') }})::float as new_prod_water_bbl,
        ({{ pv_cbm_to_bbl('volnewprodallocsand') }})::float as new_prod_sand_bbl,

        -- working interest (decimal → pct)
        ({{ pv_decimal_to_pct('wihcliq') }})::float as wi_hcliq_pct,
        ({{ pv_decimal_to_pct('wigas') }})::float as wi_gas_pct,
        ({{ pv_decimal_to_pct('wiwater') }})::float as wi_water_pct,
        ({{ pv_decimal_to_pct('wisand') }})::float as wi_sand_pct,

        -- net revenue interest (decimal → pct)
        ({{ pv_decimal_to_pct('nrihcliq') }})::float as nri_hcliq_pct,
        ({{ pv_decimal_to_pct('nrigas') }})::float as nri_gas_pct,
        ({{ pv_decimal_to_pct('nriwater') }})::float as nri_water_pct,
        ({{ pv_decimal_to_pct('nrisand') }})::float as nri_sand_pct,

        -- deferred production (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('vollosthcliq') }})::float as deferred_hcliq_bbl,
        ({{ pv_cbm_to_mcf('vollostgas') }})::float as deferred_gas_mcf,
        ({{ pv_cbm_to_bbl('vollostwater') }})::float as deferred_water_bbl,
        ({{ pv_cbm_to_bbl('vollostsand') }})::float as deferred_sand_bbl,

        -- difference from target (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('voldifftargethcliq') }})::float as diff_target_hcliq_bbl,
        ({{ pv_cbm_to_bbl('voldifftargetoil') }})::float as diff_target_oil_bbl,
        ({{ pv_cbm_to_bbl('voldifftargetcond') }})::float as diff_target_condensate_bbl,
        ({{ pv_cbm_to_bbl('voldifftargetngl') }})::float as diff_target_ngl_bbl,
        ({{ pv_cbm_to_mcf('voldifftargetgas') }})::float as diff_target_gas_mcf,
        ({{ pv_cbm_to_bbl('voldifftargetwater') }})::float as diff_target_water_bbl,
        ({{ pv_cbm_to_bbl('voldifftargetsand') }})::float as diff_target_sand_bbl,

        -- recoverable load/lift - starting volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volstartremainrecovhcliq') }})::float as starting_load_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volstartremainrecovgas') }})::float as starting_lift_gas_mcf,
        ({{ pv_cbm_to_bbl('volstartremainrecovwater') }})::float as starting_load_water_bbl,
        ({{ pv_cbm_to_bbl('volstartremainrecovsand') }})::float as starting_sand_bbl,

        -- recoverable load/lift - recovered volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volrecovhcliq') }})::float as recovered_load_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volrecovgas') }})::float as recovered_lift_gas_mcf,
        ({{ pv_cbm_to_bbl('volrecovwater') }})::float as recovered_load_water_bbl,
        ({{ pv_cbm_to_bbl('volrecovsand') }})::float as recovered_sand_bbl,

        -- recoverable load/lift - injected volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_mcf('volinjectrecovgas') }})::float as injected_lift_gas_mcf,
        ({{ pv_cbm_to_bbl('volinjectrecovhcliq') }})::float as injected_load_hcliq_bbl,
        ({{ pv_cbm_to_bbl('volinjectrecovwater') }})::float as injected_load_water_bbl,
        ({{ pv_cbm_to_bbl('volinjectrecovsand') }})::float as injected_sand_bbl,

        -- recoverable load/lift - remaining volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volremainrecovhcliq') }})::float as remaining_load_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volremainrecovgas') }})::float as remaining_lift_gas_mcf,
        ({{ pv_cbm_to_bbl('volremainrecovwater') }})::float as remaining_load_water_bbl,
        ({{ pv_cbm_to_bbl('volremainrecovsand') }})::float as remaining_sand_bbl,

        -- opening inventory (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volstartinvhcliq') }})::float as opening_inv_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volstartinvhcliqgaseq') }})::float as opening_inv_gas_eq_hcliq_mcf,
        ({{ pv_cbm_to_bbl('volstartinvwater') }})::float as opening_inv_water_bbl,
        ({{ pv_cbm_to_bbl('volstartinvsand') }})::float as opening_inv_sand_bbl,

        -- closing inventory (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volendinvhcliq') }})::float as closing_inv_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volendinvhcliqgaseq') }})::float as closing_inv_gas_eq_hcliq_mcf,
        ({{ pv_cbm_to_bbl('volendinvwater') }})::float as closing_inv_water_bbl,
        ({{ pv_cbm_to_bbl('volendinvsand') }})::float as closing_inv_sand_bbl,

        -- change in inventory (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volchginvhcliq') }})::float as chg_inv_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volchginvhcliqgaseq') }})::float as chg_inv_gas_eq_hcliq_mcf,
        ({{ pv_cbm_to_bbl('volchginvwater') }})::float as chg_inv_water_bbl,
        ({{ pv_cbm_to_bbl('volchginvsand') }})::float as chg_inv_sand_bbl,

        -- dispositions - sales (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('voldispsalehcliq') }})::float as disp_sales_hcliq_bbl,
        ({{ pv_cbm_to_bbl('voldispsaleoil') }})::float as disp_sales_oil_bbl,
        ({{ pv_cbm_to_bbl('voldispsalecond') }})::float as disp_sales_condensate_bbl,
        ({{ pv_cbm_to_bbl('voldispsalengl') }})::float as disp_sales_ngl_bbl,
        ({{ pv_cbm_to_mcf('voldispsalegas') }})::float as disp_sales_gas_mcf,

        -- dispositions - gas uses (cbm → mcf)
        ({{ pv_cbm_to_mcf('voldispfuelgas') }})::float as disp_fuel_gas_mcf,
        ({{ pv_cbm_to_mcf('voldispflaregas') }})::float as disp_flare_gas_mcf,
        ({{ pv_cbm_to_mcf('voldispincinerategas') }})::float as disp_incineration_gas_mcf,
        ({{ pv_cbm_to_mcf('voldispventgas') }})::float as disp_vent_gas_mcf,
        ({{ pv_cbm_to_mcf('voldispinjectgas') }})::float as disp_injected_gas_mcf,
        ({{ pv_cbm_to_bbl('voldispinjectwater') }})::float as disp_injected_water_bbl,

        -- injection well volumes (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volinjecthcliq') }})::float as injection_well_hcliq_bbl,
        ({{ pv_cbm_to_mcf('volinjectgas') }})::float as injection_well_gas_mcf,
        ({{ pv_cbm_to_bbl('volinjectwater') }})::float as injection_well_water_bbl,
        ({{ pv_cbm_to_bbl('volinjectsand') }})::float as injection_well_sand_bbl,

        -- cumulative production (cbm → bbl/mcf)
        ({{ pv_cbm_to_bbl('volprodcumhcliq') }})::float as cum_hcliq_bbl,
        ({{ pv_cbm_to_bbl('volprodcumoil') }})::float as cum_oil_bbl,
        ({{ pv_cbm_to_bbl('volprodcumcond') }})::float as cum_condensate_bbl,
        ({{ pv_cbm_to_bbl('volprodcumngl') }})::float as cum_ngl_bbl,
        ({{ pv_cbm_to_mcf('volprodcumgas') }})::float as cum_gas_mcf,
        ({{ pv_cbm_to_bbl('volprodcumwater') }})::float as cum_water_bbl,
        ({{ pv_cbm_to_bbl('volprodcumsand') }})::float as cum_sand_bbl,

        -- heat content (joules → mmbtu, J/m³ → btu/ft³)
        ({{ pv_joules_to_mmbtu('heatprodgath') }})::float as gathered_heat_mmbtu,
        ({{ pv_jm3_to_btu_per_ft3('factheatgath') }})::float as gathered_heat_factor_btu_per_ft3,
        ({{ pv_joules_to_mmbtu('heatprodalloc') }})::float as allocated_heat_mmbtu,
        ({{ pv_jm3_to_btu_per_ft3('factheatalloc') }})::float as allocated_heat_factor_btu_per_ft3,
        ({{ pv_joules_to_mmbtu('heatnewprodalloc') }})::float as new_prod_heat_mmbtu,
        ({{ pv_joules_to_mmbtu('heatdispsale') }})::float as disp_sales_heat_mmbtu,
        ({{ pv_joules_to_mmbtu('heatdispfuel') }})::float as disp_fuel_heat_mmbtu,
        ({{ pv_joules_to_mmbtu('heatdispflare') }})::float as disp_flare_heat_mmbtu,
        ({{ pv_joules_to_mmbtu('heatdispvent') }})::float as disp_vent_heat_mmbtu,
        ({{ pv_joules_to_mmbtu('heatdispincinerate') }})::float as disp_incinerate_heat_mmbtu,

        -- density (kg/m³ → API gravity)
        (power(nullif(densityalloc, 0), -1) / 7.07409872233005e-06 + -131.5)::float as allocated_density_api,
        (power(nullif(densitysale, 0), -1) / 7.07409872233005e-06 + -131.5)::float as sales_density_api,

        -- reference IDs for related records
        trim(idrecmeasmeth)::varchar as id_rec_meas_method,
        trim(idrecmeasmethtk)::varchar as id_rec_meas_method_tk,
        trim(idrecfluidlevel)::varchar as id_rec_fluid_level,
        trim(idrecfluidleveltk)::varchar as id_rec_fluid_level_tk,
        trim(idrectest)::varchar as id_rec_test,
        trim(idrectesttk)::varchar as id_rec_test_tk,
        trim(idrecparam)::varchar as id_rec_param,
        trim(idrecparamtk)::varchar as id_rec_param_tk,
        trim(idrecdowntime)::varchar as id_rec_downtime,
        trim(idrecdowntimetk)::varchar as id_rec_downtime_tk,
        trim(idrecdeferment)::varchar as id_rec_deferment,
        trim(idrecdefermenttk)::varchar as id_rec_deferment_tk,
        trim(idrecgasanalysis)::varchar as id_rec_gas_analysis,
        trim(idrecgasanalysistk)::varchar as id_rec_gas_analysis_tk,
        trim(idrechcliqanalysis)::varchar as id_rec_hcliq_analysis,
        trim(idrechcliqanalysistk)::varchar as id_rec_hcliq_analysis_tk,
        trim(idrecoilanalysis)::varchar as id_rec_oil_analysis,
        trim(idrecoilanalysistk)::varchar as id_rec_oil_analysis_tk,
        trim(idrecwateranalysis)::varchar as id_rec_water_analysis,
        trim(idrecwateranalysistk)::varchar as id_rec_water_analysis_tk,
        trim(idrecstatus)::varchar as id_rec_status,
        trim(idrecstatustk)::varchar as id_rec_status_tk,
        trim(idrecpumpentry)::varchar as id_rec_pump_entry,
        trim(idrecpumpentrytk)::varchar as id_rec_pump_entry_tk,
        trim(idrecfacility)::varchar as id_rec_facility,
        trim(idrecfacilitytk)::varchar as id_rec_facility_tk,
        trim(idreccalcset)::varchar as id_rec_calc_set,
        trim(idreccalcsettk)::varchar as id_rec_calc_set_tk,

        -- operational metrics
        ({{ pv_decimal_to_pct('pumpeff') }})::float as pump_efficiency_pct,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as daily_allocation_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        daily_allocation_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        id_rec_unit,
        id_rec_unit_tk,
        id_rec_comp,
        id_rec_comp_tk,
        id_rec_comp_zone,
        id_rec_comp_zone_tk,

        -- date/time
        allocation_date,
        allocation_year,
        allocation_month,
        allocation_day_of_month,

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
        allocated_gas_eq_hcliq_mcf,
        allocated_gas_mcf,
        allocated_water_bbl,
        allocated_sand_bbl,

        -- allocation factors
        alloc_factor_hcliq,
        alloc_factor_gas,
        alloc_factor_water,
        alloc_factor_sand,

        -- new production volumes
        new_prod_hcliq_bbl,
        new_prod_oil_bbl,
        new_prod_condensate_bbl,
        new_prod_ngl_bbl,
        new_prod_hcliq_gas_eq_mcf,
        new_prod_gas_mcf,
        new_prod_water_bbl,
        new_prod_sand_bbl,

        -- working interest
        wi_hcliq_pct,
        wi_gas_pct,
        wi_water_pct,
        wi_sand_pct,

        -- net revenue interest
        nri_hcliq_pct,
        nri_gas_pct,
        nri_water_pct,
        nri_sand_pct,

        -- deferred production
        deferred_hcliq_bbl,
        deferred_gas_mcf,
        deferred_water_bbl,
        deferred_sand_bbl,

        -- difference from target
        diff_target_hcliq_bbl,
        diff_target_oil_bbl,
        diff_target_condensate_bbl,
        diff_target_ngl_bbl,
        diff_target_gas_mcf,
        diff_target_water_bbl,
        diff_target_sand_bbl,

        -- recoverable load/lift - starting
        starting_load_hcliq_bbl,
        starting_lift_gas_mcf,
        starting_load_water_bbl,
        starting_sand_bbl,

        -- recoverable load/lift - recovered
        recovered_load_hcliq_bbl,
        recovered_lift_gas_mcf,
        recovered_load_water_bbl,
        recovered_sand_bbl,

        -- recoverable load/lift - injected
        injected_lift_gas_mcf,
        injected_load_hcliq_bbl,
        injected_load_water_bbl,
        injected_sand_bbl,

        -- recoverable load/lift - remaining
        remaining_load_hcliq_bbl,
        remaining_lift_gas_mcf,
        remaining_load_water_bbl,
        remaining_sand_bbl,

        -- opening inventory
        opening_inv_hcliq_bbl,
        opening_inv_gas_eq_hcliq_mcf,
        opening_inv_water_bbl,
        opening_inv_sand_bbl,

        -- closing inventory
        closing_inv_hcliq_bbl,
        closing_inv_gas_eq_hcliq_mcf,
        closing_inv_water_bbl,
        closing_inv_sand_bbl,

        -- change in inventory
        chg_inv_hcliq_bbl,
        chg_inv_gas_eq_hcliq_mcf,
        chg_inv_water_bbl,
        chg_inv_sand_bbl,

        -- dispositions - sales
        disp_sales_hcliq_bbl,
        disp_sales_oil_bbl,
        disp_sales_condensate_bbl,
        disp_sales_ngl_bbl,
        disp_sales_gas_mcf,

        -- dispositions - gas uses
        disp_fuel_gas_mcf,
        disp_flare_gas_mcf,
        disp_incineration_gas_mcf,
        disp_vent_gas_mcf,
        disp_injected_gas_mcf,
        disp_injected_water_bbl,

        -- injection well volumes
        injection_well_hcliq_bbl,
        injection_well_gas_mcf,
        injection_well_water_bbl,
        injection_well_sand_bbl,

        -- cumulative production
        cum_hcliq_bbl,
        cum_oil_bbl,
        cum_condensate_bbl,
        cum_ngl_bbl,
        cum_gas_mcf,
        cum_water_bbl,
        cum_sand_bbl,

        -- heat content
        gathered_heat_mmbtu,
        gathered_heat_factor_btu_per_ft3,
        allocated_heat_mmbtu,
        allocated_heat_factor_btu_per_ft3,
        new_prod_heat_mmbtu,
        disp_sales_heat_mmbtu,
        disp_fuel_heat_mmbtu,
        disp_flare_heat_mmbtu,
        disp_vent_heat_mmbtu,
        disp_incinerate_heat_mmbtu,

        -- density
        allocated_density_api,
        sales_density_api,

        -- reference IDs
        id_rec_meas_method,
        id_rec_meas_method_tk,
        id_rec_fluid_level,
        id_rec_fluid_level_tk,
        id_rec_test,
        id_rec_test_tk,
        id_rec_param,
        id_rec_param_tk,
        id_rec_downtime,
        id_rec_downtime_tk,
        id_rec_deferment,
        id_rec_deferment_tk,
        id_rec_gas_analysis,
        id_rec_gas_analysis_tk,
        id_rec_hcliq_analysis,
        id_rec_hcliq_analysis_tk,
        id_rec_oil_analysis,
        id_rec_oil_analysis_tk,
        id_rec_water_analysis,
        id_rec_water_analysis_tk,
        id_rec_status,
        id_rec_status_tk,
        id_rec_pump_entry,
        id_rec_pump_entry_tk,
        id_rec_facility,
        id_rec_facility_tk,
        id_rec_calc_set,
        id_rec_calc_set_tk,

        -- operational metrics
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
