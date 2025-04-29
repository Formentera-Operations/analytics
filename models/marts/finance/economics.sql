{{
    config(
        materialized='incremental',
        unique_key=['id', 'date', 'well_id'],
        cluster_by=['date', 'well_id'],
        incremental_strategy='merge',
        merge_update_columns=['extracted_at', 'econ_run_date']
    )
}}

with stg_economics as (
    select * from {{ ref('stg_cc__economic_runs') }}
),

-- Add any additional transformations here if needed
-- For example, calculating derived metrics or standardizing values

final as (
    select
        -- Identifiers and Metadata
        id,
        extracted_at,
        econ_run,
        econ_run_date,
        date,
        combo_name,
        well_id,
        project,
        scenario,
        
        -- Cash Flow Fields
        before_income_tax_cash_flow,
        after_income_tax_cash_flow,
        first_discount_cash_flow,
        second_discount_cash_flow,
        first_discount_net_income,
        second_discount_net_income,
        first_discounted_capex,
        second_discounted_capex,
        net_income,
        net_profit,
        
        -- Discount Table Cash Flow
        discount_table_cash_flow_1,
        discount_table_cash_flow_2,
        discount_table_cash_flow_3,
        discount_table_cash_flow_4,
        discount_table_cash_flow_5,
        discount_table_cash_flow_6,
        discount_table_cash_flow_7,
        discount_table_cash_flow_8,
        discount_table_cash_flow_9,
        discount_table_cash_flow_10,
        discount_table_cash_flow_11,
        discount_table_cash_flow_12,
        discount_table_cash_flow_13,
        discount_table_cash_flow_14,
        discount_table_cash_flow_15,
        discount_table_cash_flow_16,
        
        -- After-tax Discount Table Cash Flow
        afit_discount_table_cash_flow_1,
        afit_discount_table_cash_flow_2,
        afit_discount_table_cash_flow_3,
        afit_discount_table_cash_flow_4,
        afit_discount_table_cash_flow_5,
        afit_discount_table_cash_flow_6,
        afit_discount_table_cash_flow_7,
        afit_discount_table_cash_flow_8,
        afit_discount_table_cash_flow_9,
        afit_discount_table_cash_flow_10,
        afit_discount_table_cash_flow_11,
        afit_discount_table_cash_flow_12,
        afit_discount_table_cash_flow_13,
        afit_discount_table_cash_flow_14,
        afit_discount_table_cash_flow_15,
        afit_discount_table_cash_flow_16,
        
        -- Revenue Fields
        total_revenue,
        oil_revenue,
        gas_revenue,
        ngl_revenue,
        drip_condensate_revenue,
        
        -- Expense Fields
        total_expense,
        total_fixed_expense,
        total_variable_expense,
        total_oil_variable_expense,
        total_gas_variable_expense,
        total_ngl_variable_expense,
        total_drip_condensate_variable_expense,
        monthly_well_cost,
        other_monthly_cost_1,
        other_monthly_cost_2,
        
        -- Product-Specific Expenses
        oil_gathering_expense,
        oil_processing_expense,
        oil_transportation_expense,
        oil_marketing_expense,
        oil_other_expense,
        gas_gathering_expense,
        gas_processing_expense,
        gas_transportation_expense,
        gas_marketing_expense,
        gas_other_expense,
        ngl_gathering_expense,
        ngl_processing_expense,
        ngl_transportation_expense,
        ngl_marketing_expense,
        ngl_other_expense,
        drip_condensate_gathering_expense,
        drip_condensate_processing_expense,
        drip_condensate_transportation_expense,
        drip_condensate_marketing_expense,
        drip_condensate_other_expense,
        water_disposal,
        
        -- Volume Fields
        gross_boe_sales_volume,
        gross_boe_wellhead_volume,
        gross_oil_sales_volume,
        gross_oil_wellhead_volume,
        gross_gas_sales_volume,
        gross_gas_wellhead_volume,
        gross_ngl_sales_volume,
        gross_drip_condensate_sales_volume,
        gross_mcfe_sales_volume,
        gross_mcfe_wellhead_volume,
        gross_water_wellhead_volume,
        net_boe_sales_volume,
        net_oil_sales_volume,
        net_gas_sales_volume,
        net_ngl_sales_volume,
        net_drip_condensate_sales_volume,
        net_mcfe_sales_volume,
        wi_boe_sales_volume,
        wi_oil_sales_volume,
        wi_gas_sales_volume,
        wi_ngl_sales_volume,
        wi_drip_condensate_sales_volume,
        wi_mcfe_sales_volume,
        
        -- Price Fields
        oil_price,
        gas_price,
        ngl_price,
        drip_condensate_price,
        input_oil_price,
        input_gas_price,
        input_ngl_price,
        input_drip_condensate_price,
        
        -- Differentials
        oil_differentials_1,
        oil_differentials_2,
        gas_differentials_1,
        gas_differentials_2,
        ngl_differentials_1,
        ngl_differentials_2,
        drip_condensate_differentials_1,
        drip_condensate_differentials_2,
        
        -- Capital Expenditure Fields
        total_capex,
        total_intangible_capex,
        total_tangible_capex,
        total_gross_capex,
        
        -- Investment Fields
        total_other_investment,
        intangible_other_investment,
        tangible_other_investment,
        
        -- Development Capital
        total_drilling,
        intangible_drilling,
        tangible_drilling,
        total_completion,
        intangible_completion,
        tangible_completion,
        total_development,
        intangible_development,
        tangible_development,
        
        -- Facilities Capital
        total_facilities,
        intangible_facilities,
        tangible_facilities,
        total_pad,
        intangible_pad,
        tangible_pad,
        total_pipelines,
        intangible_pipelines,
        tangible_pipelines,
        total_waterline,
        intangible_waterline,
        tangible_waterline,
        
        -- Other Capital
        total_appraisal,
        intangible_appraisal,
        tangible_appraisal,
        total_exploration,
        intangible_exploration,
        tangible_exploration,
        total_artificial_lift,
        intangible_artificial_lift,
        tangible_artificial_lift,
        total_workover,
        intangible_workover,
        tangible_workover,
        total_abandonment,
        intangible_abandonment,
        tangible_abandonment,
        total_salvage,
        intangible_salvage,
        tangible_salvage,
        total_leasehold,
        intangible_leasehold,
        tangible_leasehold,
        total_legal,
        intangible_legal,
        tangible_legal,
        
        -- Tax Fields
        federal_income_tax,
        state_income_tax,
        taxable_income,
        ad_valorem_tax,
        total_production_tax,
        total_severance_tax,
        oil_severance_tax,
        gas_severance_tax,
        ngl_severance_tax,
        drip_condensate_severance_tax,
        
        -- Accounting
        depreciation,
        
        -- Production Parameters
        gross_well_count,
        nri_well_count,
        wi_well_count,
        nri_oil,
        nri_gas,
        nri_ngl,
        nri_drip_condensate,
        wi_oil,
        wi_gas,
        wi_ngl,
        wi_drip_condensate,
        lease_nri,
        
        -- Production Characteristics
        ngl_yield,
        drip_condensate_yield,
        oil_loss,
        oil_shrinkage,
        gas_loss,
        gas_shrinkage,
        gas_flare,
        
        -- Start Dates
        oil_start_using_forecast_date,
        gas_start_using_forecast_date,
        water_start_using_forecast_date,
        
        -- Derived Calculations (examples - add as needed)
        case 
            when total_capex = 0 or total_capex is null then null
            else total_revenue / nullif(total_capex, 0) 
        end as revenue_to_capex_ratio,
        
        case 
            when total_boe_prod > 0 then total_capex / nullif(total_boe_prod, 0)
            else null
        end as capex_per_boe,
        
        -- Add current timestamp for auditing
        current_timestamp() as model_loaded_at

    from (
        select
            *,
            -- Example of calculated field - total BOE production (assuming this isn't already provided)
            coalesce(net_boe_sales_volume, 0) as total_boe_prod
        from stg_economics
    )
)

select * from final

{% if is_incremental() %}
    where extracted_at > (select max(extracted_at) from {{ this }})
{% endif %}