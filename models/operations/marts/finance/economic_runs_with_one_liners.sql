

with source as (

    select * from {{ ref('int_economic_runs_with_one_liners') }}


),

final as (

    select
        -- Identifiers
        economic_run_well_id,
        economic_run_id,
        project_id,
        scenario_id,
        well_id,
        combo_name,
        economic_month as date,
        company_name as unified_company_name,
        reserves_category as unified_rsv_cat,
        operating_category as op_cat,
        
        -- Well counts
        gross_wells as producing_flag,
        net_wells as net_producing_flag,
        
        -- Gross production volumes (renamed according to cell 42)
        gross_oil_wellhead_volume as gross_oil_well_head_volume,
        gross_gas_wellhead_volume as gross_gas_well_head_volume,
        gross_water_wellhead_volume as gross_water_well_head_volume,
        gross_oil_sales_volume,
        gross_gas_sales_volume,
        gross_ngl_sales_volume,
        
        -- Working interest volumes
        wi_oil_sales_volume,
        wi_gas_sales_volume,
        wi_ngl_sales_volume,
        
        -- Net volumes (renamed according to cell 42)
        net_oil_sales_volume,
        net_gas_sales_volume as net_gas_sales_volume_mcf,
        net_mmbtu_sales_volume as net_gas_sales_volume_mmbtu,
        net_ngl_sales_volume,
        
        -- Revenue fields
        net_oil_sales_revenue,
        net_gas_sales_revenue,
        net_ngl_sales_revenue,
        
        -- Tax fields
        oil_severance_tax as stax_oil,
        gas_severance_tax as stax_gas,
        ngl_severance_tax as stax_ngl,
        ad_valorem_tax as avtax,
        
        -- Transport and expense fields
        oil_gt_deduct as oil_trans,
        gas_gt_deduct as gas_trans,
        oil_opc_expense as variable_oil_expense,
        gas_opc_expense as variable_gas_expense,
        
        -- Fixed expense fields with redistributed leftover
        fixed_expense_1 as fixed_expense_1,
        fixed_expense_2,
        fixed_expense_3,
        fixed_expense_4,
        fixed_expense_5,
        fixed_expense_6,
        fixed_expense_7,
        fixed_expense_8,
        fixed_expense_9,
        
        -- Capital expenditure fields
        monthly_total_capex as total_net_investment,
        total_gross_capex,
        
        -- Operating income and cashflow
        net_operating_income as ebitda,
        net_cashflow as cashflow,
        
        -- Metadata
        current_timestamp() as _loaded_at

    from source

)

select * from final