{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVTICKET') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idflownet)::varchar as id_flownet,

        -- ticket information
        dttm::timestamp_ntz as effective_date,
        trim(typticket)::varchar as ticket_type,
        trim(ticketno)::varchar as truck_ticket_number,
        trim(ticketsubno)::varchar as truck_ticket_sub_number,
        trim(truckco)::varchar as trucking_company,
        trim(truckcocode)::varchar as trucking_company_code,
        trim(truckrefid)::varchar as truck_number,
        exclude::boolean as is_lact_ticket,

        -- sending location
        trim(idreclocsend)::varchar as sending_location_id,
        trim(idreclocsendtk)::varchar as sending_location_table,
        trim(locsenddetail)::varchar as sending_location_detail,
        trim(idrecroutesetroutesendcalc)::varchar as sending_route_id,
        trim(idrecroutesetroutesendcalctk)::varchar as sending_route_table,
        trim(idflownetsend)::varchar as sending_flow_network_id,
        trim(idrecunitsend)::varchar as sending_unit_id,
        trim(idrecoutletsend)::varchar as sending_outlet_id,
        trim(idrecfacilitysend)::varchar as sending_facility_id,
        externalnetworksend::boolean as from_external_network,

        -- receiving location
        trim(idreclocrec)::varchar as receiving_location_id,
        trim(idreclocrectk)::varchar as receiving_location_table,
        trim(locrecdetail)::varchar as destination,
        trim(idrecroutesetroutereccalc)::varchar as receiving_route_id,
        trim(idrecroutesetroutereccalctk)::varchar as receiving_route_table,
        trim(idflownetrec)::varchar as receiving_flow_network_id,
        trim(idrecunitrec)::varchar as receiving_unit_id,
        trim(idrecinletrec)::varchar as receiving_inlet_id,
        trim(idrecfacilityrec)::varchar as receiving_facility_id,
        externalnetworkrec::boolean as to_external_network,
        recircflow::boolean as is_recirculation,

        -- basic entry volumes and quality (converted to US units)
        {{ pv_cbm_to_bbl('vol') }}::float as ticket_volume_bbl,
        {{ pv_decimal_to_pct('bsw') }}::float as ticket_bsw_pct,
        {{ pv_decimal_to_pct('sandcut') }}::float as sand_cut_pct,
        {{ pv_cbm_to_bbl('volfreewater') }}::float as free_water_volume_bbl,

        -- uncorrected volumes (converted to US units)
        {{ pv_cbm_to_bbl('voluncorrtotalcalc') }}::float as uncorrected_total_volume_bbl,
        {{ pv_cbm_to_bbl('voluncorrhcliqcalc') }}::float as uncorrected_hcliq_volume_bbl,
        {{ pv_cbm_to_bbl('voluncorrwatercalc') }}::float as uncorrected_water_volume_bbl,

        -- temperature measurements (converted to Fahrenheit)
        tempstart / 0.555555555555556 + 32 as opening_level_temperature_f,
        tempend / 0.555555555555556 + 32 as closing_level_temperature_f,
        tempsample / 0.555555555555556 + 32 as observed_temperature_f,

        -- pressure measurements (converted to PSI)
        {{ pv_kpa_to_psi('presstart') }}::float as opening_level_pressure_psi,
        {{ pv_kpa_to_psi('presend') }}::float as closing_level_pressure_psi,
        {{ pv_kpa_to_psi('pressample') }}::float as sample_pressure_psi,

        -- density measurements (converted to API gravity)
        power(nullif(densitysample, 0), -1) / 7.07409872233005e-06 + -131.5 as observed_gravity_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005e-06 + -131.5 as observed_gravity_60f_api,

        -- corrected volumes and quality (converted to US units)
        {{ pv_cbm_to_bbl('volcorrtotalcalc') }}::float as corrected_total_volume_bbl,
        {{ pv_cbm_to_bbl('volcorrhcliqcalc') }}::float as corrected_hcliq_volume_bbl,
        {{ pv_decimal_to_pct('bswcorrcalc') }}::float as corrected_bsw_pct,
        {{ pv_decimal_to_pct('sandcutcorrcalc') }}::float as corrected_sand_cut_pct,

        -- receivers override section
        trim(refid)::varchar as reference_number,
        trim(origstatementid)::varchar as statement_id,
        trim(source)::varchar as data_source,
        verified::boolean as is_verified,

        -- override conditions (converted to US units)
        tempor / 0.555555555555556 + 32 as override_temperature_f,
        {{ pv_kpa_to_psi('presor') }}::float as override_pressure_psi,
        power(nullif(densityor, 0), -1) / 7.07409872233005e-06 + -131.5 as override_density_api,

        -- override volumes (converted to US units)
        {{ pv_cbm_to_bbl('volorhcliq') }}::float as override_hcliq_volume_bbl,
        {{ pv_cbm_to_bbl('volorwater') }}::float as override_water_volume_bbl,
        {{ pv_cbm_to_bbl('volorsand') }}::float as override_sand_volume_bbl,

        -- final calculated volumes (converted to US units)
        {{ pv_cbm_to_bbl('voltotalcalc') }}::float as final_total_volume_bbl,
        {{ pv_cbm_to_bbl('volhcliqcalc') }}::float as final_oil_volume_bbl,
        {{ pv_cbm_to_mcf('volhcliqgaseqcalc') }}::float as final_gas_equivalent_liquids_mcf,
        {{ pv_cbm_to_bbl('volwatercalc') }}::float as final_water_volume_bbl,
        {{ pv_cbm_to_bbl('volsandcalc') }}::float as final_sand_volume_bbl,
        {{ pv_decimal_to_pct('bswcalc') }}::float as final_bsw_pct,
        {{ pv_decimal_to_pct('sandcutcalc') }}::float as final_sand_cut_pct,

        -- tank levels
        trim(idrectank)::varchar as tank_id,
        trim(idrectanktk)::varchar as tank_table,
        tanklevelstart::float as opening_tank_level,
        tanklevelend::float as closing_tank_level,
        {{ pv_cbm_to_bbl('tankstartvolcalc') }}::float as opening_tank_volume_bbl,
        {{ pv_cbm_to_bbl('tankendvolcalc') }}::float as closing_tank_volume_bbl,

        -- weight measurements (converted to pounds)
        trim(scaleticketno)::varchar as scale_ticket_number,
        {{ pv_kg_to_lb('weighttruckfull') }}::float as full_weight_lb,
        {{ pv_kg_to_lb('weighttruckempty') }}::float as empty_weight_lb,
        {{ pv_kg_to_lb('weightfluidcalc') }}::float as weight_of_fluid_lb,

        -- component densities
        power(nullif(compdensityhcliq, 0), -1) / 7.07409872233005e-06 + -131.5 as oil_density_api,
        {{ pv_kgm3_to_lb_per_gal('compdensitywater') }}::float as water_density_lb_per_gal,
        {{ pv_kgm3_to_lb_per_gal('compdensitysand') }}::float as sand_density_lb_per_gal,

        -- analysis and seal references
        trim(idrechcliqanalysiscalc)::varchar as hc_liquid_analysis_id,
        trim(idrechcliqanalysiscalctk)::varchar as hc_liquid_analysis_table,
        trim(idrechcliqanalysisor)::varchar as override_hc_liquid_analysis_id,
        trim(idrechcliqanalysisortk)::varchar as override_hc_liquid_analysis_table,
        trim(idrecsealentry)::varchar as seal_entry_id,
        trim(idrecsealentrytk)::varchar as seal_entry_table,

        -- system links
        trim(idrecticketlink)::varchar as linked_ticket_id,
        dttmutclastticketintrun::timestamp_ntz as last_ticket_integrator_run_utc,
        trim(noteintegrator)::varchar as ticket_integrator_notes,

        -- general information
        trim(com)::varchar as comments,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        trim(usertxt4)::varchar as user_txt4,
        trim(usertxt5)::varchar as user_txt5,
        usernum1::float as user_num1,
        usernum2::float as user_num2,
        usernum3::float as user_num3,
        usernum4::float as user_num4,
        usernum5::float as user_num5,
        userdttm1::timestamp_ntz as user_date_1,
        userdttm2::timestamp_ntz as user_date_2,
        userdttm3::timestamp_ntz as user_date_3,
        userdttm4::timestamp_ntz as user_date_4,
        userdttm5::timestamp_ntz as user_date_5,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as ticket_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        ticket_sk,

        -- identifiers
        id_rec,
        id_flownet,

        -- ticket information
        effective_date,
        ticket_type,
        truck_ticket_number,
        truck_ticket_sub_number,
        trucking_company,
        trucking_company_code,
        truck_number,
        is_lact_ticket,

        -- sending location
        sending_location_id,
        sending_location_table,
        sending_location_detail,
        sending_route_id,
        sending_route_table,
        sending_flow_network_id,
        sending_unit_id,
        sending_outlet_id,
        sending_facility_id,
        from_external_network,

        -- receiving location
        receiving_location_id,
        receiving_location_table,
        destination,
        receiving_route_id,
        receiving_route_table,
        receiving_flow_network_id,
        receiving_unit_id,
        receiving_inlet_id,
        receiving_facility_id,
        to_external_network,
        is_recirculation,

        -- basic entry volumes and quality
        ticket_volume_bbl,
        ticket_bsw_pct,
        sand_cut_pct,
        free_water_volume_bbl,

        -- uncorrected volumes
        uncorrected_total_volume_bbl,
        uncorrected_hcliq_volume_bbl,
        uncorrected_water_volume_bbl,

        -- temperature measurements
        opening_level_temperature_f,
        closing_level_temperature_f,
        observed_temperature_f,

        -- pressure measurements
        opening_level_pressure_psi,
        closing_level_pressure_psi,
        sample_pressure_psi,

        -- density measurements
        observed_gravity_api,
        observed_gravity_60f_api,

        -- corrected volumes and quality
        corrected_total_volume_bbl,
        corrected_hcliq_volume_bbl,
        corrected_bsw_pct,
        corrected_sand_cut_pct,

        -- receivers override section
        reference_number,
        statement_id,
        data_source,
        is_verified,

        -- override conditions
        override_temperature_f,
        override_pressure_psi,
        override_density_api,

        -- override volumes
        override_hcliq_volume_bbl,
        override_water_volume_bbl,
        override_sand_volume_bbl,

        -- final calculated volumes
        final_total_volume_bbl,
        final_oil_volume_bbl,
        final_gas_equivalent_liquids_mcf,
        final_water_volume_bbl,
        final_sand_volume_bbl,
        final_bsw_pct,
        final_sand_cut_pct,

        -- tank levels
        tank_id,
        tank_table,
        opening_tank_level,
        closing_tank_level,
        opening_tank_volume_bbl,
        closing_tank_volume_bbl,

        -- weight measurements
        scale_ticket_number,
        full_weight_lb,
        empty_weight_lb,
        weight_of_fluid_lb,

        -- component densities
        oil_density_api,
        water_density_lb_per_gal,
        sand_density_lb_per_gal,

        -- analysis and seal references
        hc_liquid_analysis_id,
        hc_liquid_analysis_table,
        override_hc_liquid_analysis_id,
        override_hc_liquid_analysis_table,
        seal_entry_id,
        seal_entry_table,

        -- system links
        linked_ticket_id,
        last_ticket_integrator_run_utc,
        ticket_integrator_notes,

        -- general information
        comments,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_txt4,
        user_txt5,
        user_num1,
        user_num2,
        user_num3,
        user_num4,
        user_num5,
        user_date_1,
        user_date_2,
        user_date_3,
        user_date_4,
        user_date_5,

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
