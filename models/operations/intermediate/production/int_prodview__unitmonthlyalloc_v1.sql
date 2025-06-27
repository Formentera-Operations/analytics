{{
  config(
    materialized='view',
    alias='unitmonthlyalloc_v1'
  )
}}

with pvunitallocmonth as (
    select * from {{ ref('stg_prodview__pvunitallocmonth') }}
    where deleted = false
)

select
    -- Allocation factors (dimensionless ratios)
    alloc_fact_gas,
    alloc_fact_hc_liq,
    alloc_fact_sand,
    alloc_fact_water,
    
    -- Time period
    dttm_end,
    dttm_start,
    month,
    year,
    
    -- Duration
    dur_down,
    dur_op,
    
    -- Key IDs
    id_rec,
    id_rec_parent as unit_id,
    id_rec_status as status_id,
    
    -- Net revenue interest percentages
    nri_gas,
    nri_hc_liq,
    nri_sand,
    nri_water,
    
    -- Working interest percentages
    wi_gas,
    wi_hc_liq,
    wi_sand,
    wi_water,
    
    -- System audit fields
    sys_create_date,
    sys_create_user,
    sys_mod_date,
    sys_mod_user,
    
    -- Inventory change volumes
    vol_chg_inv_hc_liq,
    vol_chg_inv_hc_liq_gas_eq,
    vol_chg_inv_sand,
    vol_chg_inv_water,
    
    -- Target difference volumes
    vol_diff_target_cond,
    vol_diff_target_gas,
    vol_diff_target_hc_liq,
    vol_diff_target_ngl,
    vol_diff_target_oil,
    vol_diff_target_sand,
    vol_diff_target_water,
    
    -- Disposition volumes
    vol_disp_flare_gas,
    vol_disp_fuel_gas,
    vol_disp_incinerate_gas,
    vol_disp_inject_gas,
    vol_disp_inject_water,
    vol_disp_sale_cond,
    vol_disp_sale_gas,
    vol_disp_sale_hc_liq,
    vol_disp_sale_ngl,
    vol_disp_sale_oil,
    vol_disp_vent_gas,
    
    -- Ending inventory volumes
    vol_end_inv_hc_liq,
    vol_end_inv_hc_liq_gas_eq,
    vol_end_inv_sand,
    vol_end_inv_water,
    
    -- Injection volumes
    vol_inject_gas,
    vol_inject_hc_liq,
    vol_inject_recov_gas,
    vol_inject_recov_hc_liq,
    vol_inject_recov_sand,
    vol_inject_recov_water,
    vol_inject_sand,
    vol_inject_water,
    
    -- Lost volumes
    vol_lost_gas,
    vol_lost_hc_liq,
    vol_lost_sand,
    vol_lost_water,
    
    -- New production allocated volumes
    vol_new_prod_alloc_cond,
    vol_new_prod_alloc_gas,
    vol_new_prod_alloc_hc_liq,
    vol_new_prod_alloc_hc_liq_gas_eq,
    vol_new_prod_alloc_ngl,
    vol_new_prod_alloc_oil,
    vol_new_prod_alloc_sand,
    vol_new_prod_alloc_water,
    
    -- Production allocated volumes
    vol_prod_alloc_cond,
    vol_prod_alloc_gas,
    vol_prod_alloc_hc_liq,
    vol_prod_alloc_hc_liq_gas_eq,
    vol_prod_alloc_ngl,
    vol_prod_alloc_oil,
    vol_prod_alloc_sand,
    vol_prod_alloc_water,
    
    -- Cumulative production volumes
    vol_prod_cum_cond,
    vol_prod_cum_gas,
    vol_prod_cum_hc_liq,
    vol_prod_cum_ngl,
    vol_prod_cum_oil,
    vol_prod_cum_sand,
    vol_prod_cum_water,
    
    -- Production gathered volumes
    vol_prod_gath_gas,
    vol_prod_gath_hc_liq,
    vol_prod_gath_sand,
    vol_prod_gath_water,
    
    -- Recovery volumes
    vol_recov_gas,
    vol_recov_hc_liq,
    vol_recov_sand,
    vol_recov_water,
    
    -- Remaining recovery volumes
    vol_remain_recov_gas,
    vol_remain_recov_hc_liq,
    vol_remain_recov_sand,
    vol_remain_recov_water,
    
    -- Starting inventory volumes
    vol_start_inv_hc_liq,
    vol_start_inv_hc_liq_gas_eq,
    vol_start_inv_sand,
    vol_start_inv_water,
    
    -- Starting remaining recovery volumes
    vol_start_remain_recov_gas,
    vol_start_remain_recov_hc_liq,
    vol_start_remain_recov_sand,
    vol_start_remain_recov_water,
    
    -- Update tracking
    update_date

from pvunitallocmonth