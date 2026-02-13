{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVFACILITYMONTHCALC') }}
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

        -- dates
        dttmstart::timestamp_ntz as period_start_date,
        dttmend::timestamp_ntz as period_end_date,
        year::float as calculation_year,
        month::float as calculation_month,

        -- production volumes (converted to US units)
        {{ pv_cbm_to_bbl('volprodhcliq') }}::float as produced_hcliq_bbl,
        {{ pv_cbm_to_mcf('volprodgas') }}::float as produced_gas_mcf,
        {{ pv_cbm_to_mcf('volprodgasplusgaseq') }}::float as produced_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volprodwater') }}::float as produced_water_bbl,
        {{ pv_cbm_to_bbl('volprodsand') }}::float as produced_sand_bbl,

        -- gathered completion volumes (converted to US units)
        {{ pv_cbm_to_bbl('volnewprodgathhcliq') }}::float as gathered_comp_hcliq_bbl,
        {{ pv_cbm_to_mcf('volnewprodgathgas') }}::float as gathered_comp_gas_mcf,
        {{ pv_cbm_to_mcf('volnewprodgathgasplusgaseq') }}::float as gathered_comp_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volnewprodgathwater') }}::float as gathered_comp_water_bbl,
        {{ pv_cbm_to_bbl('volnewprodgathsand') }}::float as gathered_comp_sand_bbl,

        -- balance factors (unitless ratios)
        balfacthcliq::float as proration_factor_hcliq,
        balfactgas::float as proration_factor_gas,
        balfactgasplusgaseq::float as proration_factor_gas_plus_gas_eq,
        balfactwater::float as proration_factor_water,
        balfactsand::float as proration_factor_sand,

        -- volume balance (converted to US units)
        {{ pv_cbm_to_bbl('volbalhcliq') }}::float as volume_balance_hcliq_bbl,
        {{ pv_cbm_to_mcf('volbalgas') }}::float as volume_balance_gas_mcf,
        {{ pv_cbm_to_mcf('volbalgasplusgaseq') }}::float as volume_balance_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volbalwater') }}::float as volume_balance_water_bbl,
        {{ pv_cbm_to_bbl('volbalsand') }}::float as volume_balance_sand_bbl,

        -- balance status flags
        balanced::boolean as all_products_balanced,
        balhcliq::boolean as hcliq_balanced,
        balgas::boolean as gas_balanced,
        balgasplusgaseq::boolean as gas_plus_gas_eq_balanced,
        balwater::boolean as water_balanced,
        balsand::boolean as sand_balanced,

        -- ins - recovered volumes (converted to US units)
        {{ pv_cbm_to_bbl('volinrecovhcliq') }}::float as recovered_load_hcliq_bbl,
        {{ pv_cbm_to_mcf('volinrecovgas') }}::float as recovered_lift_gas_mcf,
        {{ pv_cbm_to_mcf('volinrecovgasplusgaseq') }}::float as recovered_lift_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volinrecovwater') }}::float as recovered_load_water_bbl,
        {{ pv_cbm_to_bbl('volinrecovsand') }}::float as recovered_load_sand_bbl,

        -- ins - other receipts (converted to US units)
        {{ pv_cbm_to_bbl('volinotherhcliq') }}::float as receipts_in_hcliq_bbl,
        {{ pv_cbm_to_mcf('volinothergas') }}::float as receipts_in_gas_mcf,
        {{ pv_cbm_to_mcf('volinothergasplusgaseq') }}::float as receipts_in_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volinotherwater') }}::float as receipts_in_water_bbl,
        {{ pv_cbm_to_bbl('volinothersand') }}::float as receipts_in_sand_bbl,

        -- outs - consumed volumes (converted to US units)
        {{ pv_cbm_to_bbl('voloutconsumehcliq') }}::float as consumed_hcliq_bbl,
        {{ pv_cbm_to_mcf('voloutconsumegas') }}::float as consumed_gas_mcf,
        {{ pv_cbm_to_mcf('voloutconsumegasplusgaseq') }}::float as consumed_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('voloutconsumewater') }}::float as consumed_water_bbl,
        {{ pv_cbm_to_bbl('voloutconsumesand') }}::float as consumed_sand_bbl,

        -- outs - injected volumes (converted to US units)
        {{ pv_cbm_to_bbl('voloutinjectrecovhcliq') }}::float as injected_load_hcliq_bbl,
        {{ pv_cbm_to_mcf('voloutinjectrecovgas') }}::float as injected_lift_gas_mcf,
        {{ pv_cbm_to_mcf('voloutinjectrecovgasplusgaseq') }}::float as injected_lift_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('voloutinjectrecovwater') }}::float as injected_load_water_bbl,
        {{ pv_cbm_to_bbl('voloutinjectrecovsand') }}::float as injected_sand_bbl,

        -- outs - other dispositions (converted to US units)
        {{ pv_cbm_to_bbl('voloutotherhcliq') }}::float as dispositions_out_hcliq_bbl,
        {{ pv_cbm_to_mcf('voloutothergas') }}::float as dispositions_out_gas_mcf,
        {{ pv_cbm_to_mcf('voloutothergasplusgaseq') }}::float as dispositions_out_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('voloutotherwater') }}::float as dispositions_out_water_bbl,
        {{ pv_cbm_to_bbl('voloutothersand') }}::float as dispositions_out_sand_bbl,

        -- load - opening remaining volumes (converted to US units)
        {{ pv_cbm_to_bbl('volstartremainrecovhcliq') }}::float as opening_remaining_load_hcliq_bbl,
        {{ pv_cbm_to_mcf('volstartremainrecovgas') }}::float as opening_remaining_lift_gas_mcf,
        {{ pv_cbm_to_mcf('volstartremainrecovgasplusgeq') }}::float as opening_remaining_lift_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volstartremainrecovwater') }}::float as opening_remaining_load_water_bbl,
        {{ pv_cbm_to_bbl('volstartremainrecovsand') }}::float as opening_remaining_sand_bbl,

        -- load - closing remaining volumes (converted to US units)
        {{ pv_cbm_to_bbl('volendremainrecovhcliq') }}::float as closing_remaining_load_hcliq_bbl,
        {{ pv_cbm_to_mcf('volendremainrecovgas') }}::float as closing_remaining_lift_gas_mcf,
        {{ pv_cbm_to_mcf('volendremainrecovgasplusgeq') }}::float as closing_remaining_lift_gas_plus_gas_eq_mcf,
        {{ pv_cbm_to_bbl('volendremainrecovwater') }}::float as closing_remaining_load_water_bbl,
        {{ pv_cbm_to_bbl('volendremainrecovsand') }}::float as closing_remaining_sand_bbl,

        -- inventory - opening volumes (converted to US units)
        {{ pv_cbm_to_bbl('volstartinvhcliq') }}::float as opening_inventory_hcliq_bbl,
        {{ pv_cbm_to_mcf('volstartinvhcliqgaseq') }}::float as opening_inventory_gas_equivalent_hcliq_mcf,
        {{ pv_cbm_to_bbl('volstartinvwater') }}::float as opening_inventory_water_bbl,
        {{ pv_cbm_to_bbl('volstartinvsand') }}::float as opening_inventory_sand_bbl,

        -- inventory - closing volumes (converted to US units)
        {{ pv_cbm_to_bbl('volendinvhcliq') }}::float as closing_inventory_hcliq_bbl,
        {{ pv_cbm_to_mcf('volendinvhcliqgaseq') }}::float as closing_inventory_gas_equiv_hcliq_mcf,
        {{ pv_cbm_to_bbl('volendinvwater') }}::float as closing_inventory_water_bbl,
        {{ pv_cbm_to_bbl('volendinvsand') }}::float as closing_inventory_sand_bbl,

        -- inventory - change volumes (converted to US units)
        {{ pv_cbm_to_bbl('volchginvhcliq') }}::float as change_in_inventory_hcliq_bbl,
        {{ pv_cbm_to_mcf('volchginvhcliqgaseq') }}::float as change_in_inventory_gas_equivalent_hcliq_mcf,
        {{ pv_cbm_to_bbl('volchginvwater') }}::float as change_in_inventory_water_bbl,
        {{ pv_cbm_to_bbl('volchginvsand') }}::float as change_in_inventory_sand_bbl,

        -- other volumes
        {{ pv_cbm_to_mcf('volstvgas') }}::float as stv_gas_mcf,

        -- propane and butane volumes (remain in cubic meters)
        volprodpropane::float as produced_propane_m3,
        volprodbutane::float as produced_butane_m3,
        volinotherpropane::float as receipts_in_propane_m3,
        volinotherbutane::float as receipts_in_butane_m3,
        voloutotherpropane::float as dispositions_out_propane_m3,
        voloutotherbutane::float as dispositions_out_butane_m3,
        volstartpropane::float as opening_inventory_propane_m3,
        volstartbutane::float as opening_inventory_butane_m3,
        volendpropane::float as closing_inventory_propane_m3,
        volendbutane::float as closing_inventory_butane_m3,
        volchginvpropane::float as change_in_inventory_propane_m3,
        volchginvbutane::float as change_in_inventory_butane_m3,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as facility_monthly_volume_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        facility_monthly_volume_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        period_start_date,
        period_end_date,
        calculation_year,
        calculation_month,

        -- production volumes
        produced_hcliq_bbl,
        produced_gas_mcf,
        produced_gas_plus_gas_eq_mcf,
        produced_water_bbl,
        produced_sand_bbl,

        -- gathered completion volumes
        gathered_comp_hcliq_bbl,
        gathered_comp_gas_mcf,
        gathered_comp_gas_plus_gas_eq_mcf,
        gathered_comp_water_bbl,
        gathered_comp_sand_bbl,

        -- balance factors
        proration_factor_hcliq,
        proration_factor_gas,
        proration_factor_gas_plus_gas_eq,
        proration_factor_water,
        proration_factor_sand,

        -- volume balance
        volume_balance_hcliq_bbl,
        volume_balance_gas_mcf,
        volume_balance_gas_plus_gas_eq_mcf,
        volume_balance_water_bbl,
        volume_balance_sand_bbl,

        -- balance status flags
        all_products_balanced,
        hcliq_balanced,
        gas_balanced,
        gas_plus_gas_eq_balanced,
        water_balanced,
        sand_balanced,

        -- ins - recovered volumes
        recovered_load_hcliq_bbl,
        recovered_lift_gas_mcf,
        recovered_lift_gas_plus_gas_eq_mcf,
        recovered_load_water_bbl,
        recovered_load_sand_bbl,

        -- ins - other receipts
        receipts_in_hcliq_bbl,
        receipts_in_gas_mcf,
        receipts_in_gas_plus_gas_eq_mcf,
        receipts_in_water_bbl,
        receipts_in_sand_bbl,

        -- outs - consumed volumes
        consumed_hcliq_bbl,
        consumed_gas_mcf,
        consumed_gas_plus_gas_eq_mcf,
        consumed_water_bbl,
        consumed_sand_bbl,

        -- outs - injected volumes
        injected_load_hcliq_bbl,
        injected_lift_gas_mcf,
        injected_lift_gas_plus_gas_eq_mcf,
        injected_load_water_bbl,
        injected_sand_bbl,

        -- outs - other dispositions
        dispositions_out_hcliq_bbl,
        dispositions_out_gas_mcf,
        dispositions_out_gas_plus_gas_eq_mcf,
        dispositions_out_water_bbl,
        dispositions_out_sand_bbl,

        -- load - opening remaining volumes
        opening_remaining_load_hcliq_bbl,
        opening_remaining_lift_gas_mcf,
        opening_remaining_lift_gas_plus_gas_eq_mcf,
        opening_remaining_load_water_bbl,
        opening_remaining_sand_bbl,

        -- load - closing remaining volumes
        closing_remaining_load_hcliq_bbl,
        closing_remaining_lift_gas_mcf,
        closing_remaining_lift_gas_plus_gas_eq_mcf,
        closing_remaining_load_water_bbl,
        closing_remaining_sand_bbl,

        -- inventory - opening volumes
        opening_inventory_hcliq_bbl,
        opening_inventory_gas_equivalent_hcliq_mcf,
        opening_inventory_water_bbl,
        opening_inventory_sand_bbl,

        -- inventory - closing volumes
        closing_inventory_hcliq_bbl,
        closing_inventory_gas_equiv_hcliq_mcf,
        closing_inventory_water_bbl,
        closing_inventory_sand_bbl,

        -- inventory - change volumes
        change_in_inventory_hcliq_bbl,
        change_in_inventory_gas_equivalent_hcliq_mcf,
        change_in_inventory_water_bbl,
        change_in_inventory_sand_bbl,

        -- other volumes
        stv_gas_mcf,

        -- propane and butane volumes
        produced_propane_m3,
        produced_butane_m3,
        receipts_in_propane_m3,
        receipts_in_butane_m3,
        dispositions_out_propane_m3,
        dispositions_out_butane_m3,
        opening_inventory_propane_m3,
        opening_inventory_butane_m3,
        closing_inventory_propane_m3,
        closing_inventory_butane_m3,
        change_in_inventory_propane_m3,
        change_in_inventory_butane_m3,

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
