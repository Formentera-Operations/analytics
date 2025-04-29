{{
    config(
        materialized='incremental',
        unique_key=['economic_run_well_id', 'economic_run_date'],
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
        total_expense,
        total_fixed_expense,
        total_variable_expense,
        total_oil_variable_expense,
        total_gas_variable_expense,
        total_ngl_variable_expense,
        total_drip_condensate_variable_expense,
        monthly_well_cost,
        --water_disposal,

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

        -- Tax metrics
        ad_valorem_tax,
        --oil_severance_tax,
        --gas_severance_tax,
        --ngl_severance_tax,
        --drip_condensate_severance_tax,
        federal_income_tax,
        state_income_tax,
        taxable_income,

        -- Calculated metrics
        net_operating_income,
        net_cashflow,

        -- Metadata
        current_timestamp() as _loaded_at

    from source

)

select * from final