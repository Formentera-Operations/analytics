{{ config(
    materialized='view',
    tags=['prodview', 'tickets', 'transfers', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVTICKET') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as ticket_id,
        idflownet as flow_network_id,
        
        -- Date/Time and ticket information
        dttm as effective_date,
        typticket as ticket_type,
        ticketno as truck_ticket_number,
        ticketsubno as truck_ticket_sub_number,
        truckco as trucking_company,
        truckcocode as trucking_company_code,
        truckrefid as truck_number,
        exclude as is_lact_ticket,
        
        -- Sending location information
        idreclocsend as sending_location_id,
        idreclocsendtk as sending_location_table,
        locsenddetail as sending_location_detail,
        idrecroutesetroutesendcalc as sending_route_id,
        idrecroutesetroutesendcalctk as sending_route_table,
        idflownetsend as sending_flow_network_id,
        idrecunitsend as sending_unit_id,
        idrecoutletsend as sending_outlet_id,
        idrecfacilitysend as sending_facility_id,
        externalnetworksend as from_external_network,
        
        -- Receiving location information
        idreclocrec as receiving_location_id,
        idreclocrectk as receiving_location_table,
        locrecdetail as destination,
        idrecroutesetroutereccalc as receiving_route_id,
        idrecroutesetroutereccalctk as receiving_route_table,
        idflownetrec as receiving_flow_network_id,
        idrecunitrec as receiving_unit_id,
        idrecinletrec as receiving_inlet_id,
        idrecfacilityrec as receiving_facility_id,
        externalnetworkrec as to_external_network,
        recircflow as is_recirculation,
        
        -- Basic entry volumes and quality (converted to US units)
        vol / 0.158987294928 as ticket_volume_bbl,
        bsw / 0.01 as ticket_bsw_pct,
        sandcut / 0.01 as sand_cut_pct,
        volfreewater / 0.158987294928 as free_water_volume_bbl,
        
        -- Uncorrected volumes (converted to US units)
        voluncorrtotalcalc / 0.158987294928 as uncorrected_total_volume_bbl,
        voluncorrhcliqcalc / 0.158987294928 as uncorrected_hcliq_volume_bbl,
        voluncorrwatercalc / 0.158987294928 as uncorrected_water_volume_bbl,
        
        -- Temperature measurements (converted to Fahrenheit)
        tempstart / 0.555555555555556 + 32 as opening_level_temperature_f,
        tempend / 0.555555555555556 + 32 as closing_level_temperature_f,
        tempsample / 0.555555555555556 + 32 as observed_temperature_f,
        
        -- Pressure measurements (converted to PSI)
        presstart / 6.894757 as opening_level_pressure_psi,
        presend / 6.894757 as closing_level_pressure_psi,
        pressample / 6.894757 as sample_pressure_psi,
        
        -- Density measurements (converted to API gravity)
        power(nullif(densitysample, 0), -1) / 7.07409872233005E-06 + -131.5 as observed_gravity_api,
        power(nullif(densitysample60f, 0), -1) / 7.07409872233005E-06 + -131.5 as observed_gravity_60f_api,
        
        -- Corrected volumes and quality (converted to US units)
        volcorrtotalcalc / 0.158987294928 as corrected_total_volume_bbl,
        volcorrhcliqcalc / 0.158987294928 as corrected_hcliq_volume_bbl,
        bswcorrcalc / 0.01 as corrected_bsw_pct,
        sandcutcorrcalc / 0.01 as corrected_sand_cut_pct,
        
        -- Receivers override section
        refid as reference_number,
        origstatementid as statement_id,
        source as data_source,
        verified as is_verified,
        
        -- Override conditions (converted to US units)
        tempor / 0.555555555555556 + 32 as override_temperature_f,
        presor / 6.894757 as override_pressure_psi,
        power(nullif(densityor, 0), -1) / 7.07409872233005E-06 + -131.5 as override_density_api,
        
        -- Override volumes (converted to US units)
        volorhcliq / 0.158987294928 as override_hcliq_volume_bbl,
        volorwater / 0.158987294928 as override_water_volume_bbl,
        volorsand / 0.158987294928 as override_sand_volume_bbl,
        
        -- Final calculated volumes (converted to US units)
        voltotalcalc / 0.158987294928 as final_total_volume_bbl,
        volhcliqcalc / 0.158987294928 as final_oil_volume_bbl,
        volhcliqgaseqcalc / 28.316846592 as final_gas_equivalent_liquids_mcf,
        volwatercalc / 0.158987294928 as final_water_volume_bbl,
        volsandcalc / 0.158987294928 as final_sand_volume_bbl,
        bswcalc / 0.01 as final_bsw_pct,
        sandcutcalc / 0.01 as final_sand_cut_pct,
        
        -- Tank levels
        idrectank as tank_id,
        idrectanktk as tank_table,
        tanklevelstart as opening_tank_level,
        tanklevelend as closing_tank_level,
        tankstartvolcalc / 0.158987294928 as opening_tank_volume_bbl,
        tankendvolcalc / 0.158987294928 as closing_tank_volume_bbl,
        
        -- Weight measurements (converted to pounds)
        scaleticketno as scale_ticket_number,
        weighttruckfull / 0.45359237 as full_weight_lb,
        weighttruckempty / 0.45359237 as empty_weight_lb,
        weightfluidcalc / 0.45359237 as weight_of_fluid_lb,
        
        -- Component densities
        power(nullif(compdensityhcliq, 0), -1) / 7.07409872233005E-06 + -131.5 as oil_density_api,
        compdensitywater / 119.826428404623 as water_density_lb_per_gal,
        compdensitysand / 119.826428404623 as sand_density_lb_per_gal,
        
        -- Analysis and seal references
        idrechcliqanalysiscalc as hc_liquid_analysis_id,
        idrechcliqanalysiscalctk as hc_liquid_analysis_table,
        idrechcliqanalysisor as override_hc_liquid_analysis_id,
        idrechcliqanalysisortk as override_hc_liquid_analysis_table,
        idrecsealentry as seal_entry_id,
        idrecsealentrytk as seal_entry_table,
        
        -- System links
        idrecticketlink as linked_ticket_id,
        dttmutclastticketintrun as last_ticket_integrator_run_utc,
        noteintegrator as ticket_integrator_notes,
        
        -- Comments
        com as comments,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        usernum4 as user_number_4,
        usernum5 as user_number_5,
        userdttm1 as user_date_1,
        userdttm2 as user_date_2,
        userdttm3 as user_date_3,
        userdttm4 as user_date_4,
        userdttm5 as user_date_5,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed