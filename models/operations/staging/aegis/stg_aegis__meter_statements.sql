{{
    config(
        materialized='view',
        tags=['aegis', 'staging', 'hedging', 'meter_statements']
    )
}}

with

source as (

    select * from {{ source('aegis_raw', 'METER_STATEMENTS') }}

),

renamed as (

    select
        -- identifiers
        id::int as meter_statement_id,
        contractnumber::varchar as contract_number,
        primarycontractnumber::varchar as primary_contract_number,
        meternumber::varchar as meter_number,
        metername::varchar as meter_name,
        facility_name::varchar as facility_name,
        documentname::varchar as document_name,
        
        -- parties
        producer::varchar as producer_name,
        processor::varchar as processor_name,
        
        -- dates
        accountingdate::timestamp_ntz as accounting_date,
        productiondate::timestamp_ntz as production_date,
        
        -- pressure base
        pressurebase::float as pressure_base,
        
        -- ====== GAS VOLUMES (MCF/MMBTU) ======
        gross_wellhead_mcf::float as gross_wellhead_mcf,
        gross_wellhead_mmbtu::float as gross_wellhead_mmbtu,
        net_wellhead_mcf::float as net_wellhead_mcf,
        net_wellhead_mmbtu::float as net_wellhead_mmbtu,
        net_delivered_mcf::float as net_delivered_mcf,
        net_delivered_mmbtu::float as net_delivered_mmbtu,
        paystation_mcf::float as paystation_mcf,
        paystation_mmbtu::float as paystation_mmbtu,
        
        -- allocated volumes
        allocated_residue_mcf::float as allocated_residue_mcf,
        allocated_residue_mmbtu::float as allocated_residue_mmbtu,
        settlement_residue_mcf::float as settlement_residue_mcf,
        settlement_residue_mmbtu::float as settlement_residue_mmbtu,
        
        -- shrink and losses
        shrink_mcf::float as shrink_mcf,
        shrink_mmbtu::float as shrink_mmbtu,
        field_fuel_mcf::float as field_fuel_mcf,
        field_fuel_mmbtu::float as field_fuel_mmbtu,
        plant_fuel_mcf::float as plant_fuel_mcf,
        plant_fuel_mmbtu::float as plant_fuel_mmbtu,
        field_condensate_mcf::float as field_condensate_mcf,
        field_condensate_mmbtu::float as field_condensate_mmbtu,
        plant_condensate_mcf::float as plant_condensate_mcf,
        plant_condensate_mmbtu::float as plant_condensate_mmbtu,
        gas_lift_mcf::float as gas_lift_mcf,
        gas_lift_mmbtu::float as gas_lift_mmbtu,
        bypass_mcf::float as bypass_mcf,
        bypass_mmbtu::float as bypass_mmbtu,
        other_field_l_amp_u_mcf::float as other_field_loss_mcf,
        other_field_l_amp_u_mmbtu::float as other_field_loss_mmbtu,
        other_plant_l_amp_u_mcf::float as other_plant_loss_mcf,
        other_plant_l_amp_u_mmbtu::float as other_plant_loss_mmbtu,
        
        -- ====== GAS COMPOSITION (MOL %) ======
        c1_mol::float as c1_mol_pct,
        c2_mol::float as c2_mol_pct,
        c3_mol::float as c3_mol_pct,
        ic4_mol::float as ic4_mol_pct,
        nc4_mol::float as nc4_mol_pct,
        ic5_mol::float as ic5_mol_pct,
        nc5_mol::float as nc5_mol_pct,
        c6_mol::float as c6_mol_pct,
        n2_mol::float as n2_mol_pct,
        co2_mol::float as co2_mol_pct,
        h2s_mol::float as h2s_mol_pct,
        h2s_ppm::float as h2s_ppm,
        h2o_mol::float as h2o_mol_pct,
        he_mol::float as he_mol_pct,
        other_inerts_mol::float as other_inerts_mol_pct,
        total_mol::float as total_mol_pct,
        
        -- ====== NGL RECOVERY RATES (GPM) ======
        c2_gpm::float as c2_gpm,
        c3_gpm::float as c3_gpm,
        ic4_gpm::float as ic4_gpm,
        nc4_gpm::float as nc4_gpm,
        ic5_gpm::float as ic5_gpm,
        nc5_gpm::float as nc5_gpm,
        c6_gpm::float as c6_gpm,
        total_gpm::float as total_gpm,
        
        -- ====== ETHANE (C2) ======
        c2_contract_percentage::float as c2_contract_pct,
        c2_fixed_recovery::float as c2_fixed_recovery_pct,
        c2_theoretical_gallons::float as c2_theoretical_gallons,
        c2_allocated_gallons::float as c2_allocated_gallons,
        c2_settlement_gallons::float as c2_settlement_gallons,
        c2_gross_price::float as c2_gross_price,
        c2_price::float as c2_price,
        c2_t_amp_f_price::float as c2_transport_fuel_price,
        c2_shrink_mmbtu::float as c2_shrink_mmbtu,
        c2_value___statement::float as c2_value_statement,
        c2_value___tik::float as c2_value_tik,
        
        -- ====== PROPANE (C3) ======
        c3_contract_percentage::float as c3_contract_pct,
        c3_fixed_recovery::float as c3_fixed_recovery_pct,
        c3_theoretical_gallons::float as c3_theoretical_gallons,
        c3_allocated_gallons::float as c3_allocated_gallons,
        c3_settlement_gallons::float as c3_settlement_gallons,
        c3_gross_price::float as c3_gross_price,
        c3_price::float as c3_price,
        c3_t_amp_f_price::float as c3_transport_fuel_price,
        c3_shrink_mmbtu::float as c3_shrink_mmbtu,
        c3_value___statement::float as c3_value_statement,
        c3_value___tik::float as c3_value_tik,
        
        -- ====== ISO-BUTANE (IC4) ======
        ic4_contract_percentage::float as ic4_contract_pct,
        ic4_fixed_recovery::float as ic4_fixed_recovery_pct,
        ic4_theoretical_gallons::float as ic4_theoretical_gallons,
        ic4_allocated_gallons::float as ic4_allocated_gallons,
        ic4_settlement_gallons::float as ic4_settlement_gallons,
        ic4_gross_price::float as ic4_gross_price,
        ic4_price::float as ic4_price,
        ic4_t_amp_f_price::float as ic4_transport_fuel_price,
        ic4_shrink_mmbtu::float as ic4_shrink_mmbtu,
        ic4_value___statement::float as ic4_value_statement,
        ic4_value___tik::float as ic4_value_tik,
        
        -- ====== NORMAL-BUTANE (NC4) ======
        nc4_contract_percentage::float as nc4_contract_pct,
        nc4_fixed_recovery::float as nc4_fixed_recovery_pct,
        nc4_theoretical_gallons::float as nc4_theoretical_gallons,
        nc4_allocated_gallons::float as nc4_allocated_gallons,
        nc4_settlement_gallons::float as nc4_settlement_gallons,
        nc4_gross_price::float as nc4_gross_price,
        nc4_price::float as nc4_price,
        nc4_t_amp_f_price::float as nc4_transport_fuel_price,
        nc4_shrink_mmbtu::float as nc4_shrink_mmbtu,
        nc4_value___statement::float as nc4_value_statement,
        nc4_value___tik::float as nc4_value_tik,
        
        -- ====== NATURAL GASOLINE (C5+) ======
        c5_plus__contract_percentage::float as c5_plus_contract_pct,
        c5_plus__fixed_recovery::float as c5_plus_fixed_recovery_pct,
        c5_plus__theoretical_gallons::float as c5_plus_theoretical_gallons,
        c5_plus__allocated_gallons::float as c5_plus_allocated_gallons,
        c5_plus__settlement_gallons::float as c5_plus_settlement_gallons,
        c5_plus__gross_price::float as c5_plus_gross_price,
        c5_plus__price::float as c5_plus_price,
        c5_plus__t_amp_f_price::float as c5_plus_transport_fuel_price,
        c5_plus__shrink_mmbtu::float as c5_plus_shrink_mmbtu,
        c5_plus__value___statement::float as c5_plus_value_statement,
        c5_plus__value___tik::float as c5_plus_value_tik,
        
        -- ====== CONDENSATE ======
        condensate_contract_percentage::float as condensate_contract_pct,
        condensate_fixed_recovery::float as condensate_fixed_recovery_pct,
        condensate_theoretical_gallons::float as condensate_theoretical_gallons,
        condensate_allocated_gallons::float as condensate_allocated_gallons,
        condensate_settlement_gallons::float as condensate_settlement_gallons,
        condensate_gross_price::float as condensate_gross_price,
        condensate_price::float as condensate_price,
        condensate_t_amp_f_price::float as condensate_transport_fuel_price,
        condensate_shrink_mmbtu::float as condensate_shrink_mmbtu,
        condensate_value___statement::float as condensate_value_statement,
        condensate_value___tik::float as condensate_value_tik,
        
        -- ====== HELIUM ======
        helium_contract_percentage::float as helium_contract_pct,
        helium_fixed_recovery::float as helium_fixed_recovery_pct,
        helium_theoretical_gallons::float as helium_theoretical_gallons,
        helium_allocated_gallons::float as helium_allocated_gallons,
        helium_settlement_gallons::float as helium_settlement_gallons,
        helium_gross_price::float as helium_gross_price,
        helium_price::float as helium_price,
        helium_t_amp_f_price::float as helium_transport_fuel_price,
        helium_shrink_mmbtu::float as helium_shrink_mmbtu,
        helium_value___statement::float as helium_value_statement,
        helium_value___tik::float as helium_value_tik,
        
        -- ====== LIQUID TOTALS ======
        liquid_allocated_gallons::float as liquid_allocated_gallons,
        liquid_settlement_gallons::float as liquid_settlement_gallons,
        liquid_settlement_value___statement::float as liquid_value_statement,
        liquid_settlement_value___tik::float as liquid_value_tik,
        
        -- ====== RESIDUE GAS ======
        residue_contract_percent::float as residue_contract_pct,
        residue_gross_price::float as residue_gross_price,
        residue_transport_price::float as residue_transport_price,
        settlement_residue_price::float as settlement_residue_price,
        settlement_residue_value___statement::float as residue_value_statement,
        settlement_residue_value___tik::float as residue_value_tik,
        allocation_decimal::float as allocation_decimal,
        
        -- ====== GATHERING FEES ======
        gathering_fee_basis::float as gathering_fee_basis,
        gathering_fee_rate::float as gathering_fee_rate,
        gathering_fee_value::float as gathering_fee_value,
        
        -- ====== COMPRESSION FEES ======
        compression_fee_basis::float as compression_fee_basis,
        compression_fee_rate::float as compression_fee_rate,
        compression_fee_value::float as compression_fee_value,
        
        -- ====== DEHYDRATION FEES ======
        dehydration_fee_basis::float as dehydration_fee_basis,
        dehydration_fee_rate::float as dehydration_fee_rate,
        dehydration_fee_value::float as dehydration_fee_value,
        
        -- ====== PROCESSING FEES ======
        processing_fee_basis::float as processing_fee_basis,
        processing_fee_rate::float as processing_fee_rate,
        processing_fee_value::float as processing_fee_value,
        
        -- ====== TREATING FEES ======
        treating_fee_basis::float as treating_fee_basis,
        treating_fee_rate::float as treating_fee_rate,
        treating_fee_value::float as treating_fee_value,
        h2s_treating_fee_basis::float as h2s_treating_fee_basis,
        h2s_treating_fee_rate::float as h2s_treating_fee_rate,
        h2s_treating_fee_value::float as h2s_treating_fee_value,
        co2_treating_fee_basis::float as co2_treating_fee_basis,
        co2_treating_fee_rate::float as co2_treating_fee_rate,
        co2_treating_fee_value::float as co2_treating_fee_value,
        n2_treating_fee_basis::float as n2_treating_fee_basis,
        n2_treating_fee_rate::float as n2_treating_fee_rate,
        n2_treating_fee_value::float as n2_treating_fee_value,
        
        -- ====== TRANSPORTATION FEES ======
        transportation_fee_basis::float as transportation_fee_basis,
        transportation_fee_rate::float as transportation_fee_rate,
        transportation_fee_value::float as transportation_fee_value,
        ngl_transportation_fee_basis::float as ngl_transportation_fee_basis,
        ngl_transportation_fee_rate::float as ngl_transportation_fee_rate,
        ngl_transportation_fee_value::float as ngl_transportation_fee_value,
        ngl_fractionation_fee_basis::float as ngl_fractionation_fee_basis,
        ngl_fractionation_fee_rate::float as ngl_fractionation_fee_rate,
        ngl_fractionation_fee_value::float as ngl_fractionation_fee_value,
        
        -- ====== OTHER FEES ======
        electric_fee_basis::float as electric_fee_basis,
        electric_fee_rate::float as electric_fee_rate,
        electric_fee_value::float as electric_fee_value,
        gas_lift_fee_basis::float as gas_lift_fee_basis,
        gas_lift_fee_rate::float as gas_lift_fee_rate,
        gas_lift_fee_value::float as gas_lift_fee_value,
        meter_fee_basis::float as meter_fee_basis,
        meter_fee_rate::float as meter_fee_rate,
        meter_fee_value::float as meter_fee_value,
        service_fee_basis::float as service_fee_basis,
        service_fee_rate::float as service_fee_rate,
        service_fee_value::float as service_fee_value,
        low_volume_fee_basis::float as low_volume_fee_basis,
        low_volume_fee_rate::float as low_volume_fee_rate,
        low_volume_fee_value::float as low_volume_fee_value,
        minimum_proceeds_fee_basis::float as minimum_proceeds_fee_basis,
        minimum_proceeds_fee_rate::float as minimum_proceeds_fee_rate,
        minimum_proceeds_fee_value::float as minimum_proceeds_fee_value,
        marketing_fee_basis::float as marketing_fee_basis,
        marketing_fee_rate::float as marketing_fee_rate,
        marketing_fee_value::float as marketing_fee_value,
        ppa_fee_basis::float as ppa_fee_basis,
        ppa_fee_rate::float as ppa_fee_rate,
        ppa_fee_value::float as ppa_fee_value,
        capital__slash__aic_fee_basis::float as capital_aic_fee_basis,
        capital__slash__aic_fee_rate::float as capital_aic_fee_rate,
        capital__slash__aic_fee_value::float as capital_aic_fee_value,
        pressure_fee_basis::float as pressure_fee_basis,
        pressure_fee_rate::float as pressure_fee_rate,
        pressure_fee_value::float as pressure_fee_value,
        imbalance_fee_basis::float as imbalance_fee_basis,
        imbalance_fee_rate::float as imbalance_fee_rate,
        imbalance_fee_value::float as imbalance_fee_value,
        fuel_adjustment_fee_basis::float as fuel_adjustment_fee_basis,
        fuel_adjustment_fee_rate::float as fuel_adjustment_fee_rate,
        fuel_adjustment_fee_value::float as fuel_adjustment_fee_value,
        buyback_fee_basis::float as buyback_fee_basis,
        buyback_fee_rate::float as buyback_fee_rate,
        buyback_fee_value::float as buyback_fee_value,
        value_adjustment_fee_basis::float as value_adjustment_fee_basis,
        value_adjustment_fee_rate::float as value_adjustment_fee_rate,
        value_adjustment_fee_value::float as value_adjustment_fee_value,
        other_fee_basis::float as other_fee_basis,
        other_fee_rate::float as other_fee_rate,
        other_fee_value::float as other_fee_value,
        
        -- ====== TAX FEES ======
        tax_fee_basis::float as tax_fee_basis,
        tax_fee_rate::float as tax_fee_rate,
        tax_fee_value::float as tax_fee_value,
        
        -- ====== TAXES BY STATE/TYPE ======
        nmgp_taxes::float as nmgp_taxes,
        okex_taxes::float as okex_taxes,
        okmr_taxes::float as okmr_taxes,
        okpr_taxes::float as okpr_taxes,
        okrf_taxes::float as okrf_taxes,
        txgr_taxes::float as txgr_taxes,
        txrf_taxes::float as txrf_taxes,
        txsv_taxes::float as txsv_taxes,
        other_taxes::float as other_taxes,
        tax_reimbursement::float as tax_reimbursement,
        
        -- ====== SUMMARY TOTALS ======
        total_fees::float as total_fees,
        total_taxes::float as total_taxes,
        statement_net_value::float as statement_net_value,
        total_net_value::float as total_net_value,
        
        -- metadata
        _portable_extracted::timestamp_ntz as extracted_at

    from source

),

filtered as (

    select * from renamed
    where meter_statement_id is not null
        and accounting_date is not null
        and contract_number is not null

),

enhanced as (

    select
        *,
        year(accounting_date) as accounting_year,
        month(accounting_date) as accounting_month,
        year(production_date) as production_year,
        month(production_date) as production_month,
        
        -- calculate net proceeds after all deductions
        coalesce(statement_net_value, 0) - coalesce(total_fees, 0) - coalesce(total_taxes, 0) as net_proceeds,
        
        -- calculate total ngl gallons
        coalesce(c2_settlement_gallons, 0) +
        coalesce(c3_settlement_gallons, 0) +
        coalesce(ic4_settlement_gallons, 0) +
        coalesce(nc4_settlement_gallons, 0) +
        coalesce(c5_plus_settlement_gallons, 0) +
        coalesce(condensate_settlement_gallons, 0) as total_ngl_gallons,
        
        -- calculate total ngl value
        coalesce(c2_value_statement, 0) +
        coalesce(c3_value_statement, 0) +
        coalesce(ic4_value_statement, 0) +
        coalesce(nc4_value_statement, 0) +
        coalesce(c5_plus_value_statement, 0) +
        coalesce(condensate_value_statement, 0) as total_ngl_value,
        
        current_timestamp() as _loaded_at

    from filtered

),

final as (

    select
        -- identifiers
        meter_statement_id,
        contract_number,
        primary_contract_number,
        meter_number,
        meter_name,
        facility_name,
        document_name,
        producer_name,
        processor_name,
        
        -- dates
        accounting_date,
        accounting_year,
        accounting_month,
        production_date,
        production_year,
        production_month,
        
        -- pressure
        pressure_base,
        
        -- gas volumes
        gross_wellhead_mcf,
        gross_wellhead_mmbtu,
        net_wellhead_mcf,
        net_wellhead_mmbtu,
        net_delivered_mcf,
        net_delivered_mmbtu,
        paystation_mcf,
        paystation_mmbtu,
        allocated_residue_mcf,
        allocated_residue_mmbtu,
        settlement_residue_mcf,
        settlement_residue_mmbtu,
        
        -- shrink and losses
        shrink_mcf,
        shrink_mmbtu,
        field_fuel_mcf,
        field_fuel_mmbtu,
        plant_fuel_mcf,
        plant_fuel_mmbtu,
        field_condensate_mcf,
        field_condensate_mmbtu,
        plant_condensate_mcf,
        plant_condensate_mmbtu,
        gas_lift_mcf,
        gas_lift_mmbtu,
        bypass_mcf,
        bypass_mmbtu,
        other_field_loss_mcf,
        other_field_loss_mmbtu,
        other_plant_loss_mcf,
        other_plant_loss_mmbtu,
        
        -- gas composition
        c1_mol_pct,
        c2_mol_pct,
        c3_mol_pct,
        ic4_mol_pct,
        nc4_mol_pct,
        ic5_mol_pct,
        nc5_mol_pct,
        c6_mol_pct,
        n2_mol_pct,
        co2_mol_pct,
        h2s_mol_pct,
        h2s_ppm,
        h2o_mol_pct,
        he_mol_pct,
        other_inerts_mol_pct,
        total_mol_pct,
        
        -- recovery rates
        c2_gpm,
        c3_gpm,
        ic4_gpm,
        nc4_gpm,
        ic5_gpm,
        nc5_gpm,
        c6_gpm,
        total_gpm,
        
        -- ethane
        c2_contract_pct,
        c2_fixed_recovery_pct,
        c2_theoretical_gallons,
        c2_allocated_gallons,
        c2_settlement_gallons,
        c2_gross_price,
        c2_price,
        c2_transport_fuel_price,
        c2_shrink_mmbtu,
        c2_value_statement,
        c2_value_tik,
        
        -- propane
        c3_contract_pct,
        c3_fixed_recovery_pct,
        c3_theoretical_gallons,
        c3_allocated_gallons,
        c3_settlement_gallons,
        c3_gross_price,
        c3_price,
        c3_transport_fuel_price,
        c3_shrink_mmbtu,
        c3_value_statement,
        c3_value_tik,
        
        -- iso-butane
        ic4_contract_pct,
        ic4_fixed_recovery_pct,
        ic4_theoretical_gallons,
        ic4_allocated_gallons,
        ic4_settlement_gallons,
        ic4_gross_price,
        ic4_price,
        ic4_transport_fuel_price,
        ic4_shrink_mmbtu,
        ic4_value_statement,
        ic4_value_tik,
        
        -- normal-butane
        nc4_contract_pct,
        nc4_fixed_recovery_pct,
        nc4_theoretical_gallons,
        nc4_allocated_gallons,
        nc4_settlement_gallons,
        nc4_gross_price,
        nc4_price,
        nc4_transport_fuel_price,
        nc4_shrink_mmbtu,
        nc4_value_statement,
        nc4_value_tik,
        
        -- natural gasoline
        c5_plus_contract_pct,
        c5_plus_fixed_recovery_pct,
        c5_plus_theoretical_gallons,
        c5_plus_allocated_gallons,
        c5_plus_settlement_gallons,
        c5_plus_gross_price,
        c5_plus_price,
        c5_plus_transport_fuel_price,
        c5_plus_shrink_mmbtu,
        c5_plus_value_statement,
        c5_plus_value_tik,
        
        -- condensate
        condensate_contract_pct,
        condensate_fixed_recovery_pct,
        condensate_theoretical_gallons,
        condensate_allocated_gallons,
        condensate_settlement_gallons,
        condensate_gross_price,
        condensate_price,
        condensate_transport_fuel_price,
        condensate_shrink_mmbtu,
        condensate_value_statement,
        condensate_value_tik,
        
        -- helium
        helium_contract_pct,
        helium_fixed_recovery_pct,
        helium_theoretical_gallons,
        helium_allocated_gallons,
        helium_settlement_gallons,
        helium_gross_price,
        helium_price,
        helium_transport_fuel_price,
        helium_shrink_mmbtu,
        helium_value_statement,
        helium_value_tik,
        
        -- liquid totals
        liquid_allocated_gallons,
        liquid_settlement_gallons,
        liquid_value_statement,
        liquid_value_tik,
        
        -- residue gas
        residue_contract_pct,
        residue_gross_price,
        residue_transport_price,
        settlement_residue_price,
        residue_value_statement,
        residue_value_tik,
        allocation_decimal,
        
        -- gathering fees
        gathering_fee_basis,
        gathering_fee_rate,
        gathering_fee_value,
        
        -- compression fees
        compression_fee_basis,
        compression_fee_rate,
        compression_fee_value,
        
        -- dehydration fees
        dehydration_fee_basis,
        dehydration_fee_rate,
        dehydration_fee_value,
        
        -- processing fees
        processing_fee_basis,
        processing_fee_rate,
        processing_fee_value,
        
        -- treating fees
        treating_fee_basis,
        treating_fee_rate,
        treating_fee_value,
        h2s_treating_fee_basis,
        h2s_treating_fee_rate,
        h2s_treating_fee_value,
        co2_treating_fee_basis,
        co2_treating_fee_rate,
        co2_treating_fee_value,
        n2_treating_fee_basis,
        n2_treating_fee_rate,
        n2_treating_fee_value,
        
        -- transportation fees
        transportation_fee_basis,
        transportation_fee_rate,
        transportation_fee_value,
        ngl_transportation_fee_basis,
        ngl_transportation_fee_rate,
        ngl_transportation_fee_value,
        ngl_fractionation_fee_basis,
        ngl_fractionation_fee_rate,
        ngl_fractionation_fee_value,
        
        -- other fees
        electric_fee_basis,
        electric_fee_rate,
        electric_fee_value,
        gas_lift_fee_basis,
        gas_lift_fee_rate,
        gas_lift_fee_value,
        meter_fee_basis,
        meter_fee_rate,
        meter_fee_value,
        service_fee_basis,
        service_fee_rate,
        service_fee_value,
        low_volume_fee_basis,
        low_volume_fee_rate,
        low_volume_fee_value,
        minimum_proceeds_fee_basis,
        minimum_proceeds_fee_rate,
        minimum_proceeds_fee_value,
        marketing_fee_basis,
        marketing_fee_rate,
        marketing_fee_value,
        ppa_fee_basis,
        ppa_fee_rate,
        ppa_fee_value,
        capital_aic_fee_basis,
        capital_aic_fee_rate,
        capital_aic_fee_value,
        pressure_fee_basis,
        pressure_fee_rate,
        pressure_fee_value,
        imbalance_fee_basis,
        imbalance_fee_rate,
        imbalance_fee_value,
        fuel_adjustment_fee_basis,
        fuel_adjustment_fee_rate,
        fuel_adjustment_fee_value,
        buyback_fee_basis,
        buyback_fee_rate,
        buyback_fee_value,
        value_adjustment_fee_basis,
        value_adjustment_fee_rate,
        value_adjustment_fee_value,
        other_fee_basis,
        other_fee_rate,
        other_fee_value,
        
        -- tax fees
        tax_fee_basis,
        tax_fee_rate,
        tax_fee_value,
        
        -- taxes by type
        nmgp_taxes,
        okex_taxes,
        okmr_taxes,
        okpr_taxes,
        okrf_taxes,
        txgr_taxes,
        txrf_taxes,
        txsv_taxes,
        other_taxes,
        tax_reimbursement,
        
        -- summary totals
        total_fees,
        total_taxes,
        statement_net_value,
        total_net_value,
        
        -- calculated metrics
        net_proceeds,
        total_ngl_gallons,
        total_ngl_value,
        
        -- metadata
        extracted_at,
        _loaded_at

    from enhanced

)

select * from final