with economic_runs as (
    
    select * from {{ ref('stg_cc__economic_runs') }}

),

economic_one_liners as (

    select * from {{ ref('stg_cc__economic_one_liners') }}

),

joined as (

    select
        -- ids from one_liners (these are already well-named)
        one_liners.economic_run_well_id,
        one_liners.economic_one_liner_id,
        one_liners.economic_run_id,
        one_liners.project_id,
        one_liners.scenario_id,
        one_liners.well_id,
        one_liners.combo_name,
        one_liners.economic_group,
        
        -- category fields from one_liners
        one_liners.prms_reserves_category,
        one_liners.prms_reserves_subcategory,
        one_liners.prms_resources_class,
        one_liners.reserves_category,
        one_liners.company_name,
        one_liners.basin,
        one_liners.operating_category,
        
        -- summary metrics from one_liners
        one_liners.after_tax_npv_10,
        one_liners.before_tax_npv_10,
        one_liners.breakeven_oil_price,
        one_liners.breakeven_oil_price_after_tax,
        one_liners.capex,
        one_liners.cumulative_oil,
        one_liners.cumulative_gas,
        one_liners.cumulative_boe,
        one_liners.cumulative_mcfe,
        one_liners.cumulative_ngl,
        one_liners.eur_oil,
        one_liners.eur_gas,
        one_liners.eur_boe,
        one_liners.eur_mcfe,
        one_liners.eur_ngl,
        one_liners.shrunk_gas_btu,
        one_liners.first_production_date,
        one_liners.gross_wells_count,
        one_liners.irr,
        one_liners.irr_after_tax,
        one_liners.net_wells_count,
        one_liners.payout_months,
        one_liners.payout_months_after_tax,
        one_liners.total_expense,
        one_liners.total_revenue,
        one_liners.working_interest_wells_count,
        
        -- monthly data from economic_runs
        runs.econ_run_date as economic_run_date,
        runs.date as economic_month,
        runs.before_income_tax_cash_flow,
        runs.after_income_tax_cash_flow,
        runs.net_income,
        runs.net_profit,
        
        -- monthly volumes
        runs.gross_boe_sales_volume,
        runs.gross_oil_sales_volume,
        runs.gross_gas_sales_volume,
        runs.gross_ngl_sales_volume,
        runs.gross_drip_condensate_sales_volume,
        runs.gross_mcfe_sales_volume,
        
        -- monthly wellhead volumes
        runs.gross_boe_wellhead_volume,
        runs.gross_oil_wellhead_volume,
        runs.gross_gas_wellhead_volume,
        runs.gross_mcfe_wellhead_volume,
        runs.gross_water_wellhead_volume,
        
        -- well count indicator
        case 
            when runs.gross_boe_wellhead_volume > 0 then 1 
            else 0 
        end as gross_wells,
        
        -- calculated well counts
        case 
            when runs.gross_boe_wellhead_volume > 0 then 1 * runs.wi_oil / 100.0
            else 0 
        end as net_oil_wells,
        
        case 
            when runs.gross_boe_wellhead_volume > 0 then 1 * runs.wi_gas / 100.0
            else 0 
        end as net_gas_wells,
        
        greatest(
            case 
                when runs.gross_boe_wellhead_volume > 0 then 1 * runs.wi_oil / 100.0
                else 0 
            end,
            case 
                when runs.gross_boe_wellhead_volume > 0 then 1 * runs.wi_gas / 100.0
                else 0 
            end
        ) as net_wells,
        
        runs.net_boe_sales_volume,
        runs.net_oil_sales_volume,
        runs.net_gas_sales_volume,
        runs.net_ngl_sales_volume,
        runs.net_drip_condensate_sales_volume,
        runs.net_mcfe_sales_volume,
        
        -- calculated volumes
        (one_liners.shrunk_gas_btu / 1000.0) * runs.net_gas_sales_volume as net_mmbtu_sales_volume,
        
        runs.wi_boe_sales_volume,
        runs.wi_oil_sales_volume,
        runs.wi_gas_sales_volume,
        runs.wi_ngl_sales_volume,
        runs.wi_drip_condensate_sales_volume,
        runs.wi_mcfe_sales_volume,
        
        -- monthly prices
        runs.oil_price,
        runs.gas_price,
        runs.ngl_price,
        runs.drip_condensate_price,
        
        -- monthly expenses
        runs.total_expense as monthly_total_expense,
        runs.total_fixed_expense,
        runs.total_variable_expense,
        runs.total_oil_variable_expense,
        runs.total_gas_variable_expense,
        runs.total_ngl_variable_expense,
        runs.total_drip_condensate_variable_expense,
        runs.monthly_well_cost,
        
        -- monthly capital expenditure
        runs.total_capex as monthly_total_capex,
        runs.total_intangible_capex,
        runs.total_tangible_capex,
        runs.total_gross_capex,
        
        -- monthly taxes
        runs.federal_income_tax,
        runs.state_income_tax,
        runs.taxable_income,
        runs.ad_valorem_tax,
        runs.total_production_tax,
        runs.total_severance_tax,
        
        -- NRI (Net Revenue Interest) percentages
        runs.nri_oil,
        runs.nri_gas,
        runs.nri_ngl,
        runs.nri_drip_condensate,
        
        -- WI (Working Interest) percentages
        runs.wi_oil,
        runs.wi_gas,
        runs.wi_ngl,
        runs.wi_drip_condensate,

        -- Additional calculated columns
        runs.total_oil_variable_expense - runs.oil_processing_expense as leftover_oil_expense,
        runs.drip_condensate_revenue - runs.total_drip_condensate_variable_expense - runs.drip_condensate_severance_tax as leftover_condensate_revenue,
        runs.oil_revenue - (runs.total_oil_variable_expense - runs.oil_processing_expense) + 
            (runs.drip_condensate_revenue - runs.total_drip_condensate_variable_expense - runs.drip_condensate_severance_tax) as net_oil_sales_revenue,
        runs.gas_revenue as net_gas_sales_revenue,
        runs.ngl_revenue - runs.total_ngl_variable_expense as net_ngl_sales_revenue,
        0 as oil_gt_deduct,
        runs.gas_gathering_expense + runs.gas_transportation_expense as gas_gt_deduct,
        0 as ngl_gt_deduct,
        runs.oil_processing_expense as oil_opc_expense,
        runs.total_gas_variable_expense - runs.gas_gathering_expense - runs.gas_transportation_expense as gas_opc_expense,

        -- Differential Calculations
        runs.net_oil_sales_volume * runs.oil_differentials_1 as oil_diff_1_dollars,
        runs.net_gas_sales_volume * runs.gas_differentials_1 as gas_diff_1_dollars,
        runs.net_ngl_sales_volume * runs.ngl_differentials_1 as ngl_diff_1_dollars,
        runs.net_oil_sales_volume * runs.oil_differentials_2 as oil_diff_2_dollars,
        runs.net_gas_sales_volume * runs.gas_differentials_2 as gas_diff_2_dollars,
        runs.net_ngl_sales_volume * runs.ngl_differentials_2 as ngl_diff_2_dollars,

        -- Basis differential prices (differential dollars divided by volume)
        case 
            when runs.net_oil_sales_volume != 0 
            then (runs.net_oil_sales_volume * runs.oil_differentials_1) / runs.net_oil_sales_volume 
            else 0 
        end as oil_basis_diff_price_1,
        
        case 
            when net_mmbtu_sales_volume != 0 
            then (runs.net_gas_sales_volume * runs.gas_differentials_1) / net_mmbtu_sales_volume 
            else 0 
        end as gas_basis_diff_price_1,
        
        case 
            when runs.net_ngl_sales_volume != 0 
            then (runs.net_ngl_sales_volume * runs.ngl_differentials_1) / runs.net_ngl_sales_volume 
            else 0 
        end as ngl_basis_diff_price_1,
        
        case 
            when runs.net_oil_sales_volume != 0 
            then (runs.net_oil_sales_volume * runs.oil_differentials_2) / runs.net_oil_sales_volume 
            else 0 
        end as oil_basis_diff_price_2,
        
        case 
            when net_mmbtu_sales_volume != 0 
            then (runs.net_gas_sales_volume * runs.gas_differentials_2) / net_mmbtu_sales_volume 
            else 0 
        end as gas_basis_diff_price_2,
        
        case 
            when runs.net_ngl_sales_volume != 0 
            then (runs.net_ngl_sales_volume * runs.ngl_differentials_2) / runs.net_ngl_sales_volume 
            else 0 
        end as ngl_basis_diff_price_2,

        -- Net Operating Income
        (
            runs.oil_revenue + runs.gas_revenue + runs.ngl_revenue
            - oil_gt_deduct - gas_gt_deduct - ngl_gt_deduct
            - oil_opc_expense - gas_opc_expense - runs.water_disposal
            - runs.total_fixed_expense
            - runs.ad_valorem_tax - runs.oil_severance_tax - runs.gas_severance_tax
            - runs.ngl_severance_tax - runs.drip_condensate_severance_tax
        ) as net_operating_income,

        -- Net Cashflow (Net Operating Income - Total Net Investment)
        (
            runs.oil_revenue + runs.gas_revenue + runs.ngl_revenue
            - oil_gt_deduct - gas_gt_deduct - ngl_gt_deduct
            - oil_opc_expense - gas_opc_expense - runs.water_disposal
            - runs.total_fixed_expense
            - runs.ad_valorem_tax - runs.oil_severance_tax - runs.gas_severance_tax
            - runs.ngl_severance_tax - runs.drip_condensate_severance_tax
            - runs.total_capex  -- assuming this is the "Total Net Investment"
        ) as net_cashflow

    from economic_one_liners as one_liners
    inner join economic_runs as runs 
        on one_liners.economic_run_well_id = runs.economic_run_well_id

)

select * from joined