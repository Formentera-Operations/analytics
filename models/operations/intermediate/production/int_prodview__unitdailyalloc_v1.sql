{{
  config(
    materialized='view',
    alias='unitdailyalloc_v1'
  )
}}

with pvunitallocmonthday as (
    select * from {{ ref('stg_prodview__pvunitallocmonthday') }}
    where deleted = false
),

pvunitcompdowntm as (
    select * from {{ ref('stg_prodview__pvunitcompdowntm') }}
    where deleted = false
),

pvunitcompparam as (
    select * from {{ ref('stg_prodview__pvunitcompparam') }}
    where deleted = false
),

pvunitcompstatus as (
    select * from {{ ref('stg_prodview__pvunitcompstatus') }}
    where deleted = false
)

select
    -- Allocation factors (dimensionless ratios)
    pvunitallocmonthday.alloc_fact_gas,
    pvunitallocmonthday.alloc_fact_hc_liq,
    pvunitallocmonthday.alloc_fact_sand,
    pvunitallocmonthday.alloc_fact_water,
    
    -- Time period
    pvunitallocmonthday.day_of_month,
    pvunitallocmonthday.dttm,
    pvunitallocmonthday.month,
    pvunitallocmonthday.year,
    
    -- Duration
    pvunitallocmonthday.dur_down,
    pvunitallocmonthday.dur_op,
    
    -- Key IDs
    pvunitallocmonthday.id_rec_downtime as downtime_id,
    pvunitallocmonthday.id_rec_facility as facility_id,
    pvunitallocmonthday.id_rec,
    pvunitallocmonthday.id_rec_unit as unit_id,
    pvunitallocmonthday.id_rec_param as param_id,
    pvunitallocmonthday.id_rec_pump_entry as pump_id,
    pvunitallocmonthday.id_rec_status as status_id,
    pvunitallocmonthday.id_rec_test as welltest_id,
    
    -- Net revenue interest percentages
    pvunitallocmonthday.nri_gas,
    pvunitallocmonthday.nri_hc_liq,
    pvunitallocmonthday.nri_sand,
    pvunitallocmonthday.nri_water,
    
    -- Working interest percentages
    pvunitallocmonthday.wi_gas,
    pvunitallocmonthday.wi_hc_liq,
    pvunitallocmonthday.wi_sand,
    pvunitallocmonthday.wi_water,
    
    -- Pump efficiency
    pvunitallocmonthday.pump_eff,
    
    -- Complex pump token logic
    case 
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitagreemt' then 'AGREEMENT_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitagreemtpartner' then 'AGREEMENTPARTNER_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompressor' then 'COMPRESSOR_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompressorentry' then 'COMPRESSORENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvfacilitymonthdaycalc' then 'DAILYFACILITY_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitnodemonthdaycalc' then 'DAILYNODE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittankmonthdaycalc' then 'DAILYTANK_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitequip' then 'EQUIPMENT_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitequipservice' then 'EQUIPMENTSERVICE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitequipservicerec' then 'EQUIPMENTSERVICEREC_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitequipdowntm' then 'EQUIPMENTDOWNTIME_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitevent' then 'EVENT_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitevententry' then 'EVENTENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvfacility' then 'FACILITY_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvfacrecdispcalc' then 'FACILITYRECDISP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvflownetheader' then 'FLOWNETWORK_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvgasanalysis' then 'GASANALYSES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvgasanalysiscomp' then 'GASANALYSESCOMP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvgasanaly' then 'GASANALYSISGROUP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvhcliqanalysis' then 'HCLIQANALYSES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvhcliqanalysiscomp' then 'HCLIQANALYSESCOMP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvhcliqanaly' then 'HCLIQANALYSISGROUP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterpdgas' then 'METERGASPD_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterpdgasentry' then 'METERGASPDENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterliquid' then 'METERLIQUID_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterliquidentry' then 'METERLIQUIDENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterliquidfact' then 'METERLIQUIDFACTOR_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterorifice' then 'METERORIFICE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterorificeecf' then 'METERORIFICEECF_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterorificeentry' then 'METERORIFICEENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterrate' then 'METERRATE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeterrateentry' then 'METERRATEENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitnodemonthcalc' then 'MONTHLYNODE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeaspt' then 'OTHERMEASUREMENTPOINT_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitmeasptentry' then 'OTHERMEASUREMENTPOINTENTRY_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumpesp' then 'PUMPESP_V2'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumpespentry' then 'PUMPESPENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumpjet' then 'PUMPJET_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumpjetentry' then 'PUMPJETENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumppcp' then 'PUMPPCP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumppcpentry' then 'PUMPPCPENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumprod' then 'PUMPROD_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomppumprodentry' then 'PUMPRODENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitregbodykey' then 'REGBODYKEYS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitremark' then 'REMARKS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvrespteam' then 'RESPONSIBLETEAMS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvroutesetrouteuserid' then 'ROUTEUSERS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitseal' then 'SEAL_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitsealentry' then 'SEALENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittank' then 'TANK_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittankentry' then 'TANKENTRIES_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittankfactht' then 'TANKHEIGHTFACTOR_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittankstartinv' then 'TANKSTARTINV_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittankstrap' then 'TANKSTRAP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunittankstrapdata' then 'TANKSTRAPDETAILS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvticket' then 'TICKETS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunit' then 'UNIT_V2'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitallocmonthday' then 'UNITDAILYALLOCEXTENDED_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitdispmonthday' then 'UNITDAILYDISP_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitallocmonth' then 'UNITMONTHLYALLOC_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitnode' then 'UNITNODE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitnodeanaly' then 'UNITNODEANALY_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitnodecorr' then 'UNITNODECORR_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitnodenetfact' then 'UNITNODESHRINK_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitothertag' then 'UNITOTHERTAG_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvroutesetrouteunit' then 'UNITROUTE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompgathmonthdaycalc' then 'WELLDAILYGATHERED_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompdowntm' then 'WELLDOWNTIME_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompfluidlevel' then 'WELLFLUIDLEVEL_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompwhcut' then 'WELLHEADCUT_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompmeasmeth' then 'WELLMEASMETHOD_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompgathmonthcalc' then 'WELLMONTHLYGATHERED_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompparam' then 'WELLPARAM_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompratios' then 'WELLRATIOS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomptestreqexcalc' then 'WELLREQUIREDTESTSREMAINING_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompstatus' then 'WELLSTATUS_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomptarget' then 'WELLTARGET_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomptargetday' then 'WELLTARGETDAILY_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomptest' then 'WELLTEST_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcomptestreq' then 'WELLTESTINGREQUIREMENT_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompzone' then 'WELLZONE_V1'
        when pvunitallocmonthday.id_rec_pump_entry_tk = 'pvunitcompcmnglratio' then 'WELLZONERATIO_V1'
        else ''
    end as pump_tk,
    
    -- System audit fields
    pvunitallocmonthday.sys_create_date,
    pvunitallocmonthday.sys_create_user,
    pvunitallocmonthday.sys_mod_date,
    pvunitallocmonthday.sys_mod_user,
    
    -- All volume fields (keeping the same names for consistency)
    pvunitallocmonthday.vol_chg_inv_hc_liq,
    pvunitallocmonthday.vol_chg_inv_hc_liq_gas_eq,
    pvunitallocmonthday.vol_chg_inv_sand,
    pvunitallocmonthday.vol_chg_inv_water,
    pvunitallocmonthday.vol_diff_target_cond,
    pvunitallocmonthday.vol_diff_target_gas,
    pvunitallocmonthday.vol_diff_target_hc_liq,
    pvunitallocmonthday.vol_diff_target_ngl,
    pvunitallocmonthday.vol_diff_target_oil,
    pvunitallocmonthday.vol_diff_target_sand,
    pvunitallocmonthday.vol_diff_target_water,
    pvunitallocmonthday.vol_disp_flare_gas,
    pvunitallocmonthday.vol_disp_fuel_gas,
    pvunitallocmonthday.vol_disp_incinerate_gas,
    pvunitallocmonthday.vol_disp_inject_gas,
    pvunitallocmonthday.vol_disp_inject_water,
    pvunitallocmonthday.vol_disp_sale_cond,
    pvunitallocmonthday.vol_disp_sale_gas,
    pvunitallocmonthday.vol_disp_sale_hc_liq,
    pvunitallocmonthday.vol_disp_sale_ngl,
    pvunitallocmonthday.vol_disp_sale_oil,
    pvunitallocmonthday.vol_disp_vent_gas,
    pvunitallocmonthday.vol_end_inv_hc_liq,
    pvunitallocmonthday.vol_end_inv_hc_liq_gas_eq,
    pvunitallocmonthday.vol_end_inv_sand,
    pvunitallocmonthday.vol_end_inv_water,
    pvunitallocmonthday.vol_inject_gas,
    pvunitallocmonthday.vol_inject_hc_liq,
    pvunitallocmonthday.vol_inject_recov_gas,
    pvunitallocmonthday.vol_inject_recov_hc_liq,
    pvunitallocmonthday.vol_inject_recov_sand,
    pvunitallocmonthday.vol_inject_recov_water,
    pvunitallocmonthday.vol_inject_sand,
    pvunitallocmonthday.vol_inject_water,
    pvunitallocmonthday.vol_lost_gas,
    pvunitallocmonthday.vol_lost_hc_liq,
    pvunitallocmonthday.vol_lost_sand,
    pvunitallocmonthday.vol_lost_water,
    pvunitallocmonthday.vol_new_prod_alloc_cond,
    pvunitallocmonthday.vol_new_prod_alloc_gas,
    pvunitallocmonthday.vol_new_prod_alloc_hc_liq,
    pvunitallocmonthday.vol_new_prod_alloc_hc_liq_gas_eq,
    pvunitallocmonthday.vol_new_prod_alloc_ngl,
    pvunitallocmonthday.vol_new_prod_alloc_oil,
    pvunitallocmonthday.vol_new_prod_alloc_sand,
    pvunitallocmonthday.vol_new_prod_alloc_water,
    pvunitallocmonthday.vol_prod_alloc_cond,
    pvunitallocmonthday.vol_prod_alloc_gas,
    pvunitallocmonthday.vol_prod_alloc_hc_liq,
    pvunitallocmonthday.vol_prod_alloc_hc_liq_gas_eq,
    pvunitallocmonthday.vol_prod_alloc_ngl,
    pvunitallocmonthday.vol_prod_alloc_oil,
    pvunitallocmonthday.vol_prod_alloc_sand,
    pvunitallocmonthday.vol_prod_alloc_water,
    pvunitallocmonthday.vol_prod_cum_cond,
    pvunitallocmonthday.vol_prod_cum_gas,
    pvunitallocmonthday.vol_prod_cum_hc_liq,
    pvunitallocmonthday.vol_prod_cum_ngl,
    pvunitallocmonthday.vol_prod_cum_oil,
    pvunitallocmonthday.vol_prod_cum_sand,
    pvunitallocmonthday.vol_prod_cum_water,
    pvunitallocmonthday.vol_prod_gath_gas,
    pvunitallocmonthday.vol_prod_gath_hc_liq,
    pvunitallocmonthday.vol_prod_gath_sand,
    pvunitallocmonthday.vol_prod_gath_water,
    pvunitallocmonthday.vol_recov_gas,
    pvunitallocmonthday.vol_recov_hc_liq,
    pvunitallocmonthday.vol_recov_sand,
    pvunitallocmonthday.vol_recov_water,
    pvunitallocmonthday.vol_remain_recov_gas,
    pvunitallocmonthday.vol_remain_recov_hc_liq,
    pvunitallocmonthday.vol_remain_recov_sand,
    pvunitallocmonthday.vol_remain_recov_water,
    pvunitallocmonthday.vol_start_inv_hc_liq,
    pvunitallocmonthday.vol_start_inv_hc_liq_gas_eq,
    pvunitallocmonthday.vol_start_inv_sand,
    pvunitallocmonthday.vol_start_inv_water,
    pvunitallocmonthday.vol_start_remain_recov_gas,
    pvunitallocmonthday.vol_start_remain_recov_hc_liq,
    pvunitallocmonthday.vol_start_remain_recov_sand,
    pvunitallocmonthday.vol_start_remain_recov_water,
    
    -- Joined data from downtime table
    pvunitcompdowntm.code_downtime_1,
    pvunitcompdowntm.code_downtime_2,
    pvunitcompdowntm.code_downtime_3,
    
    -- Joined data from parameter table
    pvunitcompparam.ph,
    pvunitcompparam.pres_bottomhole as pres_bh,
    pvunitcompparam.pres_casing as pres_cas,
    pvunitcompparam.pres_casing_si as pres_cas_si,
    pvunitcompparam.pres_injection as pres_inj,
    pvunitcompparam.pres_line,
    pvunitcompparam.pres_tubing as pres_tub,
    pvunitcompparam.pres_tubing_si as pres_tub_si,
    pvunitcompparam.pres_wellhead as pres_wh,
    pvunitcompparam.salinity,
    pvunitcompparam.size_choke as sz_choke,
    pvunitcompparam.temp_bottomhole as temp_bh,
    pvunitcompparam.temp_wellhead as temp_wh,
    pvunitcompparam.visc_dynamic,
    pvunitcompparam.visc_kinematic,
    
    -- Joined data from status table
    pvunitcompstatus.status,
    
    -- Update date (maximum of all joined tables)
    greatest(
        coalesce(pvunitallocmonthday.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(pvunitcompdowntm.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(pvunitcompparam.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz),
        coalesce(pvunitcompstatus.update_date, '0000-01-01T00:00:00.000Z'::timestamp_tz)
    ) as update_date

from pvunitallocmonthday
left join pvunitcompdowntm 
    on pvunitallocmonthday.id_rec_downtime = pvunitcompdowntm.id_rec
left join pvunitcompparam 
    on pvunitallocmonthday.id_rec_param = pvunitcompparam.id_rec
left join pvunitcompstatus 
    on pvunitallocmonthday.id_rec_status = pvunitcompstatus.id_rec