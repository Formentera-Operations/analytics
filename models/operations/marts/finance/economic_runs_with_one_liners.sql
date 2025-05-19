{{
    config(
        materialized='incremental',
        unique_key='economic_run_well_id',
        on_schema_change='sync_all_columns',
        incremental_strategy='merge'
    )
}} 

with source as (

    select * from {{ ref('int_economic_runs_with_one_liners') }}

    {% if is_incremental() %}
    where economic_run_date >= (select max(economic_run_date) from {{ this }})
    {% endif %}

),

final as (

    select
        -- Identifiers
        economic_run_well_id,  -- Added this missing column
        economic_one_liner_id,
        economic_run_id,
        project_id,
        scenario_id,
        well_id,
        combo_name,
        economic_group,
        economic_run_date,
        economic_month,

        -- Category fields
        prms_reserves_category,
        prms_reserves_subcategory,
        prms_resources_class,
        reserves_category,
        company_name,
        basin,
        operating_category,

                -- NRI (Net Revenue Interest) percentages
        nri_oil,
        nri_gas,
        nri_ngl,
        nri_drip_condensate,
        
        -- WI (Working Interest) percentages
        wi_oil,
        wi_gas,
        wi_ngl,
        wi_drip_condensate,

        -- Financial metrics
        before_income_tax_cash_flow,
        after_income_tax_cash_flow,
        net_income,
        net_profit,
        
        -- Revenue metrics
        total_revenue,
        net_oil_sales_revenue,
        net_gas_sales_revenue,
        net_ngl_sales_revenue,
        
        -- Expense metrics
            --total_expense,
        monthly_total_expense,
        total_fixed_expense,
        total_variable_expense,
        total_oil_variable_expense,
        total_gas_variable_expense,
        total_ngl_variable_expense,
        total_drip_condensate_variable_expense,
        monthly_well_cost,
        --water_disposal,

        -- monthly capital expenditure
        monthly_total_capex,
        total_intangible_capex,
        total_tangible_capex,
        total_gross_capex,

        -- Calculated fields
        leftover_oil_expense,
        leftover_condensate_revenue,
        oil_gt_deduct,
        gas_gt_deduct,
        ngl_gt_deduct,
        oil_opc_expense,
        gas_opc_expense,

        -- Sales volumes
        net_boe_sales_volume,
        net_oil_sales_volume,
        net_gas_sales_volume,
        net_ngl_sales_volume,
        net_drip_condensate_sales_volume,
        net_mcfe_sales_volume,
        net_mmbtu_sales_volume,
        wi_boe_sales_volume,
        wi_oil_sales_volume,
        wi_gas_sales_volume,
        wi_ngl_sales_volume,
        wi_drip_condensate_sales_volume,
        wi_mcfe_sales_volume,

        -- Price metrics
        oil_price,
        gas_price,
        ngl_price,
        drip_condensate_price,

        -- Basis differential metrics
        oil_basis_diff_price_1,
        gas_basis_diff_price_1,
        ngl_basis_diff_price_1,
        oil_basis_diff_price_2,
        gas_basis_diff_price_2,
        ngl_basis_diff_price_2,

        -- Differential Calculations
        oil_diff_1_dollars,
        gas_diff_1_dollars,
        ngl_diff_1_dollars,
        oil_diff_2_dollars,
        gas_diff_2_dollars,
        ngl_diff_2_dollars,

        -- Tax metrics
        ad_valorem_tax,
        --oil_severance_tax,
        --gas_severance_tax,
        --ngl_severance_tax,
        --drip_condensate_severance_tax,
        federal_income_tax,
        state_income_tax,
        taxable_income,
        total_production_tax,
        total_severance_tax,

        -- Calculated metrics
        net_operating_income,
        net_cashflow,

        -- summary metrics from one_liners
        after_tax_npv_10,
        before_tax_npv_10,
        breakeven_oil_price,
        breakeven_oil_price_after_tax,
        capex,
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
        shrunk_gas_btu,
        first_production_date,
        gross_wells_count,
        irr,
        irr_after_tax,
        net_wells_count,
        payout_months,
        payout_months_after_tax,
        working_interest_wells_count,

        -- monthly volumes
        gross_boe_sales_volume,
        gross_oil_sales_volume,
        gross_gas_sales_volume,
        gross_ngl_sales_volume,
        gross_drip_condensate_sales_volume,
        gross_mcfe_sales_volume,

        -- monthly wellhead volumes
        gross_boe_wellhead_volume,
        gross_oil_wellhead_volume,
        gross_gas_wellhead_volume,
        gross_mcfe_wellhead_volume,
        gross_water_wellhead_volume,

        -- well count indicator
        gross_wells,
        net_oil_wells,
        net_gas_wells,
        net_wells,

        -- Metadata
        current_timestamp() as _loaded_at

    from source

)

select * from final