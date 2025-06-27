

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITALLOCMONTHDAY') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        
        -- Unit and completion references
        IDRECUNIT as id_rec_unit,
        IDRECUNITTK as id_rec_unit_tk,
        IDRECCOMP as id_rec_comp,
        IDRECCOMPTK as id_rec_comp_tk,
        IDRECCOMPZONE as id_rec_comp_zone,
        IDRECCOMPZONETK as id_rec_comp_zone_tk,
        
        -- Time period (daily granularity)
        DTTM as dttm,
        YEAR as year,
        MONTH as month,
        DAYOFMONTH as day_of_month,
        
        -- Duration with unit conversion (minutes to hours)
        DURDOWN / 0.0416666666666667 as dur_down,
        case when DURDOWN is not null then 'HR' else null end as dur_down_unit_label,
        DUROP / 0.0416666666666667 as dur_op,
        case when DUROP is not null then 'HR' else null end as dur_op_unit_label,
        
        -- Production gathered volumes (cubic meters to barrels/MCF)
        VOLPRODGATHHCLIQ / 0.158987294928 as vol_prod_gath_hc_liq,
        case when VOLPRODGATHHCLIQ is not null then 'BBL' else null end as vol_prod_gath_hc_liq_unit_label,
        VOLPRODGATHGAS / 28.316846592 as vol_prod_gath_gas,
        case when VOLPRODGATHGAS is not null then 'MCF' else null end as vol_prod_gath_gas_unit_label,
        VOLPRODGATHWATER / 0.158987294928 as vol_prod_gath_water,
        case when VOLPRODGATHWATER is not null then 'BBL' else null end as vol_prod_gath_water_unit_label,
        VOLPRODGATHSAND / 0.158987294928 as vol_prod_gath_sand,
        case when VOLPRODGATHSAND is not null then 'BBL' else null end as vol_prod_gath_sand_unit_label,
        
        -- Production allocated volumes
        VOLPRODALLOCHCLIQ / 0.158987294928 as vol_prod_alloc_hc_liq,
        case when VOLPRODALLOCHCLIQ is not null then 'BBL' else null end as vol_prod_alloc_hc_liq_unit_label,
        VOLPRODALLOCOIL / 0.158987294928 as vol_prod_alloc_oil,
        case when VOLPRODALLOCOIL is not null then 'BBL' else null end as vol_prod_alloc_oil_unit_label,
        VOLPRODALLOCCOND / 0.158987294928 as vol_prod_alloc_cond,
        case when VOLPRODALLOCCOND is not null then 'BBL' else null end as vol_prod_alloc_cond_unit_label,
        VOLPRODALLOCNGL / 0.158987294928 as vol_prod_alloc_ngl,
        case when VOLPRODALLOCNGL is not null then 'BBL' else null end as vol_prod_alloc_ngl_unit_label,
        VOLPRODALLOCHCLIQGASEQ / 28.316846592 as vol_prod_alloc_hc_liq_gas_eq,
        case when VOLPRODALLOCHCLIQGASEQ is not null then 'MCF' else null end as vol_prod_alloc_hc_liq_gas_eq_unit_label,
        VOLPRODALLOCGAS / 28.316846592 as vol_prod_alloc_gas,
        case when VOLPRODALLOCGAS is not null then 'MCF' else null end as vol_prod_alloc_gas_unit_label,
        VOLPRODALLOCWATER / 0.158987294928 as vol_prod_alloc_water,
        case when VOLPRODALLOCWATER is not null then 'BBL' else null end as vol_prod_alloc_water_unit_label,
        VOLPRODALLOCSAND / 0.158987294928 as vol_prod_alloc_sand,
        case when VOLPRODALLOCSAND is not null then 'BBL' else null end as vol_prod_alloc_sand_unit_label,
        
        -- Allocation factors (dimensionless ratios)
        ALLOCFACTHCLIQ as alloc_fact_hc_liq,
        case when ALLOCFACTHCLIQ is not null then 'M³/M³' else null end as alloc_fact_hc_liq_unit_label,
        ALLOCFACTGAS as alloc_fact_gas,
        case when ALLOCFACTGAS is not null then 'M³/M³' else null end as alloc_fact_gas_unit_label,
        ALLOCFACTWATER as alloc_fact_water,
        case when ALLOCFACTWATER is not null then 'M³/M³' else null end as alloc_fact_water_unit_label,
        ALLOCFACTSAND as alloc_fact_sand,
        case when ALLOCFACTSAND is not null then 'M³/M³' else null end as alloc_fact_sand_unit_label,
        
        -- New production allocated volumes
        VOLNEWPRODALLOCHCLIQ / 0.158987294928 as vol_new_prod_alloc_hc_liq,
        case when VOLNEWPRODALLOCHCLIQ is not null then 'BBL' else null end as vol_new_prod_alloc_hc_liq_unit_label,
        VOLNEWPRODALLOCOIL / 0.158987294928 as vol_new_prod_alloc_oil,
        case when VOLNEWPRODALLOCOIL is not null then 'BBL' else null end as vol_new_prod_alloc_oil_unit_label,
        VOLNEWPRODALLOCCOND / 0.158987294928 as vol_new_prod_alloc_cond,
        case when VOLNEWPRODALLOCCOND is not null then 'BBL' else null end as vol_new_prod_alloc_cond_unit_label,
        VOLNEWPRODALLOCNGL / 0.158987294928 as vol_new_prod_alloc_ngl,
        case when VOLNEWPRODALLOCNGL is not null then 'BBL' else null end as vol_new_prod_alloc_ngl_unit_label,
        VOLNEWPRODALLOCHCLIQGASEQ / 28.316846592 as vol_new_prod_alloc_hc_liq_gas_eq,
        case when VOLNEWPRODALLOCHCLIQGASEQ is not null then 'MCF' else null end as vol_new_prod_alloc_hc_liq_gas_eq_unit_label,
        VOLNEWPRODALLOCGAS / 28.316846592 as vol_new_prod_alloc_gas,
        case when VOLNEWPRODALLOCGAS is not null then 'MCF' else null end as vol_new_prod_alloc_gas_unit_label,
        VOLNEWPRODALLOCWATER / 0.158987294928 as vol_new_prod_alloc_water,
        case when VOLNEWPRODALLOCWATER is not null then 'BBL' else null end as vol_new_prod_alloc_water_unit_label,
        VOLNEWPRODALLOCSAND / 0.158987294928 as vol_new_prod_alloc_sand,
        case when VOLNEWPRODALLOCSAND is not null then 'BBL' else null end as vol_new_prod_alloc_sand_unit_label,
        
        -- Working interest percentages (decimal to percentage)
        WIHCLIQ / 0.01 as wi_hc_liq,
        case when WIHCLIQ is not null then '%' else null end as wi_hc_liq_unit_label,
        WIGAS / 0.01 as wi_gas,
        case when WIGAS is not null then '%' else null end as wi_gas_unit_label,
        WIWATER / 0.01 as wi_water,
        case when WIWATER is not null then '%' else null end as wi_water_unit_label,
        WISAND / 0.01 as wi_sand,
        case when WISAND is not null then '%' else null end as wi_sand_unit_label,
        
        -- Net revenue interest percentages
        NRIHCLIQ / 0.01 as nri_hc_liq,
        case when NRIHCLIQ is not null then '%' else null end as nri_hc_liq_unit_label,
        NRIGAS / 0.01 as nri_gas,
        case when NRIGAS is not null then '%' else null end as nri_gas_unit_label,
        NRIWATER / 0.01 as nri_water,
        case when NRIWATER is not null then '%' else null end as nri_water_unit_label,
        NRISAND / 0.01 as nri_sand,
        case when NRISAND is not null then '%' else null end as nri_sand_unit_label,
        
        -- Lost volumes
        VOLLOSTHCLIQ / 0.158987294928 as vol_lost_hc_liq,
        case when VOLLOSTHCLIQ is not null then 'BBL' else null end as vol_lost_hc_liq_unit_label,
        VOLLOSTGAS / 28.316846592 as vol_lost_gas,
        case when VOLLOSTGAS is not null then 'MCF' else null end as vol_lost_gas_unit_label,
        VOLLOSTWATER / 0.158987294928 as vol_lost_water,
        case when VOLLOSTWATER is not null then 'BBL' else null end as vol_lost_water_unit_label,
        VOLLOSTSAND / 0.158987294928 as vol_lost_sand,
        case when VOLLOSTSAND is not null then 'BBL' else null end as vol_lost_sand_unit_label,
        
        -- Target difference volumes
        VOLDIFFTARGETHCLIQ / 0.158987294928 as vol_diff_target_hc_liq,
        case when VOLDIFFTARGETHCLIQ is not null then 'BBL' else null end as vol_diff_target_hc_liq_unit_label,
        VOLDIFFTARGETOIL / 0.158987294928 as vol_diff_target_oil,
        case when VOLDIFFTARGETOIL is not null then 'BBL' else null end as vol_diff_target_oil_unit_label,
        VOLDIFFTARGETCOND / 0.158987294928 as vol_diff_target_cond,
        case when VOLDIFFTARGETCOND is not null then 'BBL' else null end as vol_diff_target_cond_unit_label,
        VOLDIFFTARGETNGL / 0.158987294928 as vol_diff_target_ngl,
        case when VOLDIFFTARGETNGL is not null then 'BBL' else null end as vol_diff_target_ngl_unit_label,
        VOLDIFFTARGETGAS / 28.316846592 as vol_diff_target_gas,
        case when VOLDIFFTARGETGAS is not null then 'MCF' else null end as vol_diff_target_gas_unit_label,
        VOLDIFFTARGETWATER / 0.158987294928 as vol_diff_target_water,
        case when VOLDIFFTARGETWATER is not null then 'BBL' else null end as vol_diff_target_water_unit_label,
        VOLDIFFTARGETSAND / 0.158987294928 as vol_diff_target_sand,
        case when VOLDIFFTARGETSAND is not null then 'BBL' else null end as vol_diff_target_sand_unit_label,
        
        -- Starting remaining recovery volumes
        VOLSTARTREMAINRECOVHCLIQ / 0.158987294928 as vol_start_remain_recov_hc_liq,
        case when VOLSTARTREMAINRECOVHCLIQ is not null then 'BBL' else null end as vol_start_remain_recov_hc_liq_unit_label,
        VOLSTARTREMAINRECOVGAS / 28.316846592 as vol_start_remain_recov_gas,
        case when VOLSTARTREMAINRECOVGAS is not null then 'MCF' else null end as vol_start_remain_recov_gas_unit_label,
        VOLSTARTREMAINRECOVWATER / 0.158987294928 as vol_start_remain_recov_water,
        case when VOLSTARTREMAINRECOVWATER is not null then 'BBL' else null end as vol_start_remain_recov_water_unit_label,
        VOLSTARTREMAINRECOVSAND / 0.158987294928 as vol_start_remain_recov_sand,
        case when VOLSTARTREMAINRECOVSAND is not null then 'BBL' else null end as vol_start_remain_recov_sand_unit_label,
        
        -- Recovery volumes
        VOLRECOVHCLIQ / 0.158987294928 as vol_recov_hc_liq,
        case when VOLRECOVHCLIQ is not null then 'BBL' else null end as vol_recov_hc_liq_unit_label,
        VOLRECOVGAS / 28.316846592 as vol_recov_gas,
        case when VOLRECOVGAS is not null then 'MCF' else null end as vol_recov_gas_unit_label,
        VOLRECOVWATER / 0.158987294928 as vol_recov_water,
        case when VOLRECOVWATER is not null then 'BBL' else null end as vol_recov_water_unit_label,
        VOLRECOVSAND / 0.158987294928 as vol_recov_sand,
        case when VOLRECOVSAND is not null then 'BBL' else null end as vol_recov_sand_unit_label,
        
        -- Injection recovery volumes
        VOLINJECTRECOVGAS / 28.316846592 as vol_inject_recov_gas,
        case when VOLINJECTRECOVGAS is not null then 'MCF' else null end as vol_inject_recov_gas_unit_label,
        VOLINJECTRECOVHCLIQ / 0.158987294928 as vol_inject_recov_hc_liq,
        case when VOLINJECTRECOVHCLIQ is not null then 'BBL' else null end as vol_inject_recov_hc_liq_unit_label,
        VOLINJECTRECOVWATER / 0.158987294928 as vol_inject_recov_water,
        case when VOLINJECTRECOVWATER is not null then 'BBL' else null end as vol_inject_recov_water_unit_label,
        VOLINJECTRECOVSAND / 0.158987294928 as vol_inject_recov_sand,
        case when VOLINJECTRECOVSAND is not null then 'BBL' else null end as vol_inject_recov_sand_unit_label,
        
        -- Remaining recovery volumes
        VOLREMAINRECOVHCLIQ / 0.158987294928 as vol_remain_recov_hc_liq,
        case when VOLREMAINRECOVHCLIQ is not null then 'BBL' else null end as vol_remain_recov_hc_liq_unit_label,
        VOLREMAINRECOVGAS / 28.316846592 as vol_remain_recov_gas,
        case when VOLREMAINRECOVGAS is not null then 'MCF' else null end as vol_remain_recov_gas_unit_label,
        VOLREMAINRECOVWATER / 0.158987294928 as vol_remain_recov_water,
        case when VOLREMAINRECOVWATER is not null then 'BBL' else null end as vol_remain_recov_water_unit_label,
        VOLREMAINRECOVSAND / 0.158987294928 as vol_remain_recov_sand,
        case when VOLREMAINRECOVSAND is not null then 'BBL' else null end as vol_remain_recov_sand_unit_label,
        
        -- Starting inventory volumes
        VOLSTARTINVHCLIQ / 0.158987294928 as vol_start_inv_hc_liq,
        case when VOLSTARTINVHCLIQ is not null then 'BBL' else null end as vol_start_inv_hc_liq_unit_label,
        VOLSTARTINVHCLIQGASEQ / 28.316846592 as vol_start_inv_hc_liq_gas_eq,
        case when VOLSTARTINVHCLIQGASEQ is not null then 'MCF' else null end as vol_start_inv_hc_liq_gas_eq_unit_label,
        VOLSTARTINVWATER / 0.158987294928 as vol_start_inv_water,
        case when VOLSTARTINVWATER is not null then 'BBL' else null end as vol_start_inv_water_unit_label,
        VOLSTARTINVSAND / 0.158987294928 as vol_start_inv_sand,
        case when VOLSTARTINVSAND is not null then 'BBL' else null end as vol_start_inv_sand_unit_label,
        
        -- Ending inventory volumes
        VOLENDINVHCLIQ / 0.158987294928 as vol_end_inv_hc_liq,
        case when VOLENDINVHCLIQ is not null then 'BBL' else null end as vol_end_inv_hc_liq_unit_label,
        VOLENDINVHCLIQGASEQ / 28.316846592 as vol_end_inv_hc_liq_gas_eq,
        case when VOLENDINVHCLIQGASEQ is not null then 'MCF' else null end as vol_end_inv_hc_liq_gas_eq_unit_label,
        VOLENDINVWATER / 0.158987294928 as vol_end_inv_water,
        case when VOLENDINVWATER is not null then 'BBL' else null end as vol_end_inv_water_unit_label,
        VOLENDINVSAND / 0.158987294928 as vol_end_inv_sand,
        case when VOLENDINVSAND is not null then 'BBL' else null end as vol_end_inv_sand_unit_label,
        
        -- Inventory change volumes
        VOLCHGINVHCLIQ / 0.158987294928 as vol_chg_inv_hc_liq,
        case when VOLCHGINVHCLIQ is not null then 'BBL' else null end as vol_chg_inv_hc_liq_unit_label,
        VOLCHGINVHCLIQGASEQ / 28.316846592 as vol_chg_inv_hc_liq_gas_eq,
        case when VOLCHGINVHCLIQGASEQ is not null then 'MCF' else null end as vol_chg_inv_hc_liq_gas_eq_unit_label,
        VOLCHGINVWATER / 0.158987294928 as vol_chg_inv_water,
        case when VOLCHGINVWATER is not null then 'BBL' else null end as vol_chg_inv_water_unit_label,
        VOLCHGINVSAND / 0.158987294928 as vol_chg_inv_sand,
        case when VOLCHGINVSAND is not null then 'BBL' else null end as vol_chg_inv_sand_unit_label,
        
        -- Disposition sale volumes
        VOLDISPSALEHCLIQ / 0.158987294928 as vol_disp_sale_hc_liq,
        case when VOLDISPSALEHCLIQ is not null then 'BBL' else null end as vol_disp_sale_hc_liq_unit_label,
        VOLDISPSALEOIL / 0.158987294928 as vol_disp_sale_oil,
        case when VOLDISPSALEOIL is not null then 'BBL' else null end as vol_disp_sale_oil_unit_label,
        VOLDISPSALECOND / 0.158987294928 as vol_disp_sale_cond,
        case when VOLDISPSALECOND is not null then 'BBL' else null end as vol_disp_sale_cond_unit_label,
        VOLDISPSALENGL / 0.158987294928 as vol_disp_sale_ngl,
        case when VOLDISPSALENGL is not null then 'BBL' else null end as vol_disp_sale_ngl_unit_label,
        VOLDISPSALEGAS / 28.316846592 as vol_disp_sale_gas,
        case when VOLDISPSALEGAS is not null then 'MCF' else null end as vol_disp_sale_gas_unit_label,
        
        -- Gas disposition volumes
        VOLDISPFUELGAS / 28.316846592 as vol_disp_fuel_gas,
        case when VOLDISPFUELGAS is not null then 'MCF' else null end as vol_disp_fuel_gas_unit_label,
        VOLDISPFLAREGAS / 28.316846592 as vol_disp_flare_gas,
        case when VOLDISPFLAREGAS is not null then 'MCF' else null end as vol_disp_flare_gas_unit_label,
        VOLDISPINCINERATEGAS / 28.316846592 as vol_disp_incinerate_gas,
        case when VOLDISPINCINERATEGAS is not null then 'MCF' else null end as vol_disp_incinerate_gas_unit_label,
        VOLDISPVENTGAS / 28.316846592 as vol_disp_vent_gas,
        case when VOLDISPVENTGAS is not null then 'MCF' else null end as vol_disp_vent_gas_unit_label,
        VOLDISPINJECTGAS / 28.316846592 as vol_disp_inject_gas,
        case when VOLDISPINJECTGAS is not null then 'MCF' else null end as vol_disp_inject_gas_unit_label,
        VOLDISPINJECTWATER / 0.158987294928 as vol_disp_inject_water,
        case when VOLDISPINJECTWATER is not null then 'BBL' else null end as vol_disp_inject_water_unit_label,
        
        -- Injection volumes
        VOLINJECTHCLIQ / 0.158987294928 as vol_inject_hc_liq,
        case when VOLINJECTHCLIQ is not null then 'BBL' else null end as vol_inject_hc_liq_unit_label,
        VOLINJECTGAS / 28.316846592 as vol_inject_gas,
        case when VOLINJECTGAS is not null then 'MCF' else null end as vol_inject_gas_unit_label,
        VOLINJECTWATER / 0.158987294928 as vol_inject_water,
        case when VOLINJECTWATER is not null then 'BBL' else null end as vol_inject_water_unit_label,
        VOLINJECTSAND / 0.158987294928 as vol_inject_sand,
        case when VOLINJECTSAND is not null then 'BBL' else null end as vol_inject_sand_unit_label,
        
        -- Cumulative production volumes
        VOLPRODCUMHCLIQ / 0.158987294928 as vol_prod_cum_hc_liq,
        case when VOLPRODCUMHCLIQ is not null then 'BBL' else null end as vol_prod_cum_hc_liq_unit_label,
        VOLPRODCUMOIL / 0.158987294928 as vol_prod_cum_oil,
        case when VOLPRODCUMOIL is not null then 'BBL' else null end as vol_prod_cum_oil_unit_label,
        VOLPRODCUMCOND / 0.158987294928 as vol_prod_cum_cond,
        case when VOLPRODCUMCOND is not null then 'BBL' else null end as vol_prod_cum_cond_unit_label,
        VOLPRODCUMNGL / 0.158987294928 as vol_prod_cum_ngl,
        case when VOLPRODCUMNGL is not null then 'BBL' else null end as vol_prod_cum_ngl_unit_label,
        VOLPRODCUMGAS / 28.316846592 as vol_prod_cum_gas,
        case when VOLPRODCUMGAS is not null then 'MCF' else null end as vol_prod_cum_gas_unit_label,
        VOLPRODCUMWATER / 0.158987294928 as vol_prod_cum_water,
        case when VOLPRODCUMWATER is not null then 'BBL' else null end as vol_prod_cum_water_unit_label,
        VOLPRODCUMSAND / 0.158987294928 as vol_prod_cum_sand,
        case when VOLPRODCUMSAND is not null then 'BBL' else null end as vol_prod_cum_sand_unit_label,
        
        -- Heat content and heating values
        HEATPRODGATH / 1055055852.62 as heat_prod_gath,
        case when HEATPRODGATH is not null then 'MMBTU' else null end as heat_prod_gath_unit_label,
        FACTHEATGATH / 37258.9458078313 as fact_heat_gath,
        case when FACTHEATGATH is not null then 'BTU/FT³' else null end as fact_heat_gath_unit_label,
        HEATPRODALLOC / 1055055852.62 as heat_prod_alloc,
        case when HEATPRODALLOC is not null then 'MMBTU' else null end as heat_prod_alloc_unit_label,
        FACTHEATALLOC / 37258.9458078313 as fact_heat_alloc,
        case when FACTHEATALLOC is not null then 'BTU/FT³' else null end as fact_heat_alloc_unit_label,
        HEATNEWPRODALLOC / 1055055852.62 as heat_new_prod_alloc,
        case when HEATNEWPRODALLOC is not null then 'MMBTU' else null end as heat_new_prod_alloc_unit_label,
        HEATDISPSALE / 1055055852.62 as heat_disp_sale,
        case when HEATDISPSALE is not null then 'MMBTU' else null end as heat_disp_sale_unit_label,
        HEATDISPFUEL / 1055055852.62 as heat_disp_fuel,
        case when HEATDISPFUEL is not null then 'MMBTU' else null end as heat_disp_fuel_unit_label,
        HEATDISPFLARE / 1055055852.62 as heat_disp_flare,
        case when HEATDISPFLARE is not null then 'MMBTU' else null end as heat_disp_flare_unit_label,
        HEATDISPVENT / 1055055852.62 as heat_disp_vent,
        case when HEATDISPVENT is not null then 'MMBTU' else null end as heat_disp_vent_unit_label,
        HEATDISPINCINERATE / 1055055852.62 as heat_disp_incinerate,
        case when HEATDISPINCINERATE is not null then 'MMBTU' else null end as heat_disp_incinerate_unit_label,
        
        -- Density conversions (complex formula to API gravity)
        power(nullif(DENSITYALLOC, 0), -1) / 7.07409872233005E-06 + -131.5 as density_alloc,
        case when DENSITYALLOC is not null then '°API' else null end as density_alloc_unit_label,
        power(nullif(DENSITYSALE, 0), -1) / 7.07409872233005E-06 + -131.5 as density_sale,
        case when DENSITYSALE is not null then '°API' else null end as density_sale_unit_label,
        
        -- Reference IDs
        IDRECMEASMETH as id_rec_meas_meth,
        IDRECMEASMETHTK as id_rec_meas_meth_tk,
        IDRECFLUIDLEVEL as id_rec_fluid_level,
        IDRECFLUIDLEVELTK as id_rec_fluid_level_tk,
        IDRECTEST as id_rec_test,
        IDRECTESTTK as id_rec_test_tk,
        IDRECPARAM as id_rec_param,
        IDRECPARAMTK as id_rec_param_tk,
        IDRECDOWNTIME as id_rec_downtime,
        IDRECDOWNTIMETK as id_rec_downtime_tk,
        IDRECDEFERMENT as id_rec_deferment,
        IDRECDEFERMENTTK as id_rec_deferment_tk,
        IDRECGASANALYSIS as id_rec_gas_analysis,
        IDRECGASANALYSISTK as id_rec_gas_analysis_tk,
        IDRECHCLIQANALYSIS as id_rec_hc_liq_analysis,
        IDRECHCLIQANALYSISTK as id_rec_hc_liq_analysis_tk,
        IDRECOILANALYSIS as id_rec_oil_analysis,
        IDRECOILANALYSISTK as id_rec_oil_analysis_tk,
        IDRECWATERANALYSIS as id_rec_water_analysis,
        IDRECWATERANALYSISTK as id_rec_water_analysis_tk,
        IDRECSTATUS as id_rec_status,
        IDRECSTATUSTK as id_rec_status_tk,
        IDRECPUMPENTRY as id_rec_pump_entry,
        IDRECPUMPENTRYTK as id_rec_pump_entry_tk,
        IDRECFACILITY as id_rec_facility,
        IDRECFACILITYTK as id_rec_facility_tk,
        IDRECCALCSET as id_rec_calc_set,
        IDRECCALCSETTK as id_rec_calc_set_tk,
        
        -- Pump efficiency (decimal to percentage)
        PUMPEFF / 0.01 as pump_eff,
        case when PUMPEFF is not null then '%' else null end as pump_eff_unit_label,
        
        -- System locking fields
        SYSLOCKMEUI as sys_lock_me_ui,
        SYSLOCKCHILDRENUI as sys_lock_children_ui,
        SYSLOCKME as sys_lock_me,
        SYSLOCKCHILDREN as sys_lock_children,
        SYSLOCKDATE as sys_lock_date,
        
        -- System audit fields
        SYSMODDATE as sys_mod_date,
        SYSMODUSER as sys_mod_user,
        SYSCREATEDATE as sys_create_date,
        SYSCREATEUSER as sys_create_user,
        SYSTAG as sys_tag,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed