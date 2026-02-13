{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPGATHMONTHDAYCALC') }}
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

        -- dates
        dttm::timestamp_ntz as gathered_date,
        year::int as gathered_year,
        month::int as gathered_month,
        dayofmonth::int as day_of_month,

        -- operational time
        {{ pv_days_to_hours('durop') }}::float as operating_time_hours,
        {{ pv_days_to_hours('durdown') }}::float as downtime_hours,

        -- gathered production volumes
        {{ pv_cbm_to_bbl('voltotalliq') }}::float as total_liquid_bbl,
        {{ pv_cbm_to_bbl('volhcliq') }}::float as gathered_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volgas') }}::float as gathered_gas_mcf,
        {{ pv_cbm_to_bbl('volwater') }}::float as gathered_water_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as gathered_sand_bbl,
        {{ pv_cbm_to_mcf('volcasinggas') }}::float as casing_gas_mcf,

        -- new gathered production
        {{ pv_cbm_to_bbl('volnewprodgathhcliq') }}::float as new_gathered_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volnewprodgathgas') }}::float as new_gathered_gas_mcf,
        {{ pv_cbm_to_bbl('volnewprodgathwater') }}::float as new_gathered_water_bbl,
        {{ pv_cbm_to_bbl('volnewprodgathsand') }}::float as new_gathered_sand_bbl,

        -- prorated production
        {{ pv_cbm_to_bbl('volproratedhcliq') }}::float as prorated_hcliq_bbl,
        {{ pv_cbm_to_mcf('volproratedgas') }}::float as prorated_gas_mcf,
        {{ pv_cbm_to_bbl('volproratedwater') }}::float as prorated_water_bbl,
        {{ pv_cbm_to_bbl('volproratedsand') }}::float as prorated_sand_bbl,

        -- recoverable load/lift - starting volumes
        {{ pv_cbm_to_bbl('volgathstartremainrecovhcliq') }}::float as starting_load_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volgathstartremainrecovgas') }}::float as starting_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volgathstartremainrecovwater') }}::float as starting_load_water_bbl,
        {{ pv_cbm_to_bbl('volgathstartremainrecovsand') }}::float as starting_sand_bbl,

        -- recoverable load/lift - recovered volumes
        {{ pv_cbm_to_bbl('volgathrecovhcliq') }}::float as recovered_load_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volgathrecovgas') }}::float as recovered_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volgathrecovwater') }}::float as recovered_load_water_bbl,
        {{ pv_cbm_to_bbl('volgathrecovsand') }}::float as recovered_sand_bbl,

        -- recoverable load/lift - injected volumes
        {{ pv_cbm_to_mcf('volgathinjectrecovgas') }}::float as injected_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volgathinjectrecovhcliq') }}::float as injected_load_oil_condensate_bbl,
        {{ pv_cbm_to_bbl('volgathinjectrecovwater') }}::float as injected_load_water_bbl,
        {{ pv_cbm_to_bbl('volgathinjectrecovsand') }}::float as injected_sand_bbl,

        -- recoverable load/lift - remaining volumes
        {{ pv_cbm_to_bbl('volgathremainrecovhcliq') }}::float as remaining_load_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('volgathremainrecovgas') }}::float as remaining_lift_gas_mcf,
        {{ pv_cbm_to_bbl('volgathremainrecovwater') }}::float as remaining_load_water_bbl,
        {{ pv_cbm_to_bbl('volgathremainrecovsand') }}::float as remaining_sand_bbl,

        -- fluid properties
        {{ pv_cbm_ratio_to_mcf_per_bbl('gor') }}::float as gas_oil_ratio_mcf_per_bbl,

        -- production rates
        {{ pv_cbm_to_bbl_per_day('ratetotalliq') }}::float as rate_total_liquid_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratehcliq') }}::float as rate_oil_condensate_bbl_per_day,
        {{ pv_cbm_to_mcf('rategas') }}::float as gas_rate_mcf_per_day,
        {{ pv_cbm_to_bbl_per_day('ratewater') }}::float as water_rate_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratesand') }}::float as sand_rate_bbl_per_day,

        -- change in rates
        {{ pv_cbm_to_bbl_per_day('ratechgtotalliq') }}::float as change_in_total_liquid_rate_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratechghcliq') }}::float as change_in_oil_condensate_rate_bbl_per_day,
        {{ pv_cbm_to_mcf('ratechggas') }}::float as change_in_gas_rate_mcf_per_day,
        {{ pv_cbm_to_bbl_per_day('ratechgwater') }}::float as change_in_water_rate_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratechgsand') }}::float as change_in_sand_rate_bbl_per_day,

        -- percent change in rates
        {{ pv_decimal_to_pct('pctchgtotliq') }}::float as pct_change_total_liquid_rate_pct,
        {{ pv_decimal_to_pct('pctchghcliq') }}::float as pct_change_oil_condensate_rate_pct,
        {{ pv_decimal_to_pct('pctchggas') }}::float as pct_change_gas_rate_pct,
        {{ pv_decimal_to_pct('pctchgwater') }}::float as pct_change_water_rate_pct,
        {{ pv_decimal_to_pct('pctchgsand') }}::float as pct_change_sand_rate_pct,

        -- rate tolerance flags
        rateintol::boolean as all_products_rate_within_tolerance,
        ratehcliqintol::boolean as oil_condensate_rate_within_tolerance,
        rategasintol::boolean as gas_rate_within_tolerance,
        ratewaterintol::boolean as water_rate_within_tolerance,
        ratesandintol::boolean as sand_rate_within_tolerance,

        -- lost production due to downtime
        {{ pv_cbm_to_bbl('vollosthcliq') }}::float as deferred_oil_condensate_production_bbl,
        {{ pv_cbm_to_mcf('vollostgas') }}::float as deferred_gas_production_mcf,
        {{ pv_cbm_to_bbl('vollostwater') }}::float as deferred_water_production_bbl,
        {{ pv_cbm_to_bbl('vollostsand') }}::float as deferred_sand_production_bbl,

        -- difference from target
        {{ pv_cbm_to_bbl('voldifftargethcliq') }}::float as difference_from_target_oil_condensate_bbl,
        {{ pv_cbm_to_mcf('voldifftargetgas') }}::float as difference_from_target_gas_mcf,
        {{ pv_cbm_to_bbl('voldifftargetwater') }}::float as difference_from_target_water_bbl,
        {{ pv_cbm_to_bbl('voldifftargetsand') }}::float as difference_from_target_sand_bbl,

        -- injection volumes
        {{ pv_cbm_to_bbl('volinjecthcliq') }}::float as injection_well_oil_cond_bbl,
        {{ pv_cbm_to_mcf('volinjectgas') }}::float as injection_well_gas_mcf,
        {{ pv_cbm_to_bbl('volinjectwater') }}::float as injection_well_water_bbl,
        {{ pv_cbm_to_bbl('volinjectsand') }}::float as injection_well_sand_bbl,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as gathered_daily_volume_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        gathered_daily_volume_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        completion_id,
        completion_table,

        -- dates
        gathered_date,
        gathered_year,
        gathered_month,
        day_of_month,

        -- operational time
        operating_time_hours,
        downtime_hours,

        -- gathered production volumes
        total_liquid_bbl,
        gathered_oil_condensate_bbl,
        gathered_gas_mcf,
        gathered_water_bbl,
        gathered_sand_bbl,
        casing_gas_mcf,

        -- new gathered production
        new_gathered_oil_condensate_bbl,
        new_gathered_gas_mcf,
        new_gathered_water_bbl,
        new_gathered_sand_bbl,

        -- prorated production
        prorated_hcliq_bbl,
        prorated_gas_mcf,
        prorated_water_bbl,
        prorated_sand_bbl,

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

        -- fluid properties
        gas_oil_ratio_mcf_per_bbl,

        -- production rates
        rate_total_liquid_bbl_per_day,
        rate_oil_condensate_bbl_per_day,
        gas_rate_mcf_per_day,
        water_rate_bbl_per_day,
        sand_rate_bbl_per_day,

        -- change in rates
        change_in_total_liquid_rate_bbl_per_day,
        change_in_oil_condensate_rate_bbl_per_day,
        change_in_gas_rate_mcf_per_day,
        change_in_water_rate_bbl_per_day,
        change_in_sand_rate_bbl_per_day,

        -- percent change in rates
        pct_change_total_liquid_rate_pct,
        pct_change_oil_condensate_rate_pct,
        pct_change_gas_rate_pct,
        pct_change_water_rate_pct,
        pct_change_sand_rate_pct,

        -- rate tolerance flags
        all_products_rate_within_tolerance,
        oil_condensate_rate_within_tolerance,
        gas_rate_within_tolerance,
        water_rate_within_tolerance,
        sand_rate_within_tolerance,

        -- lost production due to downtime
        deferred_oil_condensate_production_bbl,
        deferred_gas_production_mcf,
        deferred_water_production_bbl,
        deferred_sand_production_bbl,

        -- difference from target
        difference_from_target_oil_condensate_bbl,
        difference_from_target_gas_mcf,
        difference_from_target_water_bbl,
        difference_from_target_sand_bbl,

        -- injection volumes
        injection_well_oil_cond_bbl,
        injection_well_gas_mcf,
        injection_well_water_bbl,
        injection_well_sand_bbl,

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
