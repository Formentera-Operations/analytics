with economic_data as (

    select * from {{ ref('stg_cc__economic_one_liners') }}

),

final as (

    select
        -- ids
        economic_one_liner_id,
        economic_run_id,
        project_id,
        scenario_id,
        well_id,
        combo_name,
        economic_group,
        
        -- categorization
        prms_reserves_category,
        prms_reserves_subcategory,
        prms_resources_class,
        reserves_category,
        company_name,
        basin,
        operating_category,
        
        -- production metrics
        first_production_date,
        gross_wells_count,
        net_wells_count,
        working_interest_wells_count,
        
        -- volume metrics
        cumulative_oil,
        cumulative_gas,
        cumulative_boe,
        cumulative_mcfe,
        cumulative_ngl,
        eur_oil,
        eur_gas,
        eur_boe,
        eur_mcfe,
        eur_ngl,
        
        -- financial metrics
        capex,
        total_expense,
        total_revenue,
        net_operating_income,
        before_income_tax_cash_flow,
        
        -- economic indicators
        after_tax_npv_10,
        before_tax_npv_10,
        breakeven_oil_price,
        breakeven_oil_price_after_tax,
        irr,
        irr_after_tax,
        payout_months,
        payout_months_after_tax,
        
        -- reference metrics
        reference_after_tax_npv_10,
        reference_before_tax_npv_10,
        reference_breakeven_oil_price,
        reference_breakeven_oil_price_after_tax,
        reference_capex,
        reference_total_expense,
        reference_total_revenue,
        reference_irr,
        reference_irr_after_tax,
        reference_payout_months,
        reference_payout_months_after_tax,
        
        -- calculated fields
        round((after_tax_npv_10 - reference_after_tax_npv_10) / nullif(reference_after_tax_npv_10, 0) * 100, 2) as after_tax_npv_10_pct_change,
        round((before_tax_npv_10 - reference_before_tax_npv_10) / nullif(reference_before_tax_npv_10, 0) * 100, 2) as before_tax_npv_10_pct_change,
        round((irr - reference_irr) / nullif(reference_irr, 0) * 100, 2) as irr_pct_change,
        round((capex - reference_capex) / nullif(reference_capex, 0) * 100, 2) as capex_pct_change

    from economic_data

)

select * from final