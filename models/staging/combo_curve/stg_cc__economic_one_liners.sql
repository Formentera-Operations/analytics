with source as (

    select * from {{ source('combo_curve', 'econ_run_one_liners') }}

),

surrogate_key as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'econrun',
            'well',
            'project',
            'scenario',
            'comboname'
        ]) }} as economic_run_well_id
    from source

),

renamed as (

    select
        -- ids
        id as economic_one_liner_id,
        econrun as economic_run_id,
        project as project_id,
        scenario as scenario_id,
        well as well_id,
        comboname as combo_name,
        econgroup as economic_group,
        economic_run_well_id,
        
        -- category fields
        econprmsreservescategory as prms_reserves_category,
        econprmsreservessubcategory as prms_reserves_subcategory,
        econprmsresourcesclass as prms_resources_class,
        
        -- metrics extracted from json
        get(output, 'after_tax_npv_10.0')::float as after_tax_npv_10,
        get(output, 'before_tax_npv_10.0')::float as before_tax_npv_10,
        get(output, 'breakeven_oil_price')::float as breakeven_oil_price,
        get(output, 'breakeven_oil_price_after_tax')::float as breakeven_oil_price_after_tax,
        get(output, 'capex')::float as capex,
        get(output, 'cum_oil')::float as cumulative_oil,
        get(output, 'cum_gas')::float as cumulative_gas,
        get(output, 'cum_boe')::float as cumulative_boe,
        get(output, 'cum_mcfe')::float as cumulative_mcfe,
        get(output, 'cum_ngl')::float as cumulative_ngl,
        get(output, 'eur_oil')::float as eur_oil,
        get(output, 'eur_gas')::float as eur_gas,
        get(output, 'eur_boe')::float as eur_boe,
        get(output, 'eur_mcfe')::float as eur_mcfe,
        get(output, 'eur_ngl')::float as eur_ngl,
        get(output, 'first_production')::date as first_production_date,
        get(output, 'gross_wells')::float as gross_wells_count,
        get(output, 'irr')::float as irr,
        get(output, 'irr_after_tax')::float as irr_after_tax,
        get(output, 'net_wells')::float as net_wells_count,
        get(output, 'payout_months')::float as payout_months,
        get(output, 'payout_months_after_tax')::float as payout_months_after_tax,
        get(output, 'total_expense')::float as total_expense,
        get(output, 'total_revenue')::float as total_revenue,
        get(output, 'wi_wells')::float as working_interest_wells_count,
        get(output, 'chosen_id')::string as chosen_id,
        get(output, 'basin')::string as basin,
        get(output, 'op_cat')::string as operating_category,
        get(output, 'shrunk_gas_btu')::float as shrunk_gas_btu,
        {{ transform_company_name("get(output, 'company_name')::string") }} as company_name,
        get(output, 'net_operating_income')::float as net_operating_income,
        get(output, 'before_income_tax_cash_flow')::float as before_income_tax_cash_flow,
        {{ transform_reserve_category("get(output, 'rsv_cat')::string") }} as reserves_category,
        
        -- reference values from json
        get(output, 'reference_after_tax_npv_10.0')::float as reference_after_tax_npv_10,
        get(output, 'reference_before_tax_npv_10.0')::float as reference_before_tax_npv_10,
        get(output, 'reference_breakeven_oil_price')::float as reference_breakeven_oil_price,
        get(output, 'reference_breakeven_oil_price_after_tax')::float as reference_breakeven_oil_price_after_tax,
        get(output, 'reference_capex')::float as reference_capex,
        get(output, 'reference_cum_oil')::float as reference_cumulative_oil,
        get(output, 'reference_cum_gas')::float as reference_cumulative_gas,
        get(output, 'reference_cum_boe')::float as reference_cumulative_boe,
        get(output, 'reference_cum_mcfe')::float as reference_cumulative_mcfe,
        get(output, 'reference_cum_ngl')::float as reference_cumulative_ngl,
        get(output, 'reference_eur_oil')::float as reference_eur_oil,
        get(output, 'reference_eur_gas')::float as reference_eur_gas,
        get(output, 'reference_eur_boe')::float as reference_eur_boe,
        get(output, 'reference_eur_mcfe')::float as reference_eur_mcfe,
        get(output, 'reference_eur_ngl')::float as reference_eur_ngl,
        get(output, 'reference_irr')::float as reference_irr,
        get(output, 'reference_irr_after_tax')::float as reference_irr_after_tax,
        get(output, 'reference_payout_months')::float as reference_payout_months,
        get(output, 'reference_payout_months_after_tax')::float as reference_payout_months_after_tax,
        get(output, 'reference_total_expense')::float as reference_total_expense,
        get(output, 'reference_total_revenue')::float as reference_total_revenue,
        
        -- raw json (kept for debugging or future parsing)
        output as economic_output_raw,
        
        -- metadata
        _portable_extracted as portable_extracted

    from surrogate_key

)

select * from renamed