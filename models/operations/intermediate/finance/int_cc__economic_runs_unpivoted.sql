{{
    config(
        enabled=false,
        materialized='table'
    )
}}

with source as (
    select * from {{ ref('stg_cc__economic_runs') }}
),

/*
    Unpivot revenue metrics
*/
revenue_metrics as (
    select
        economic_run_well_id,
        date,
        'revenue' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            total_revenue,
            oil_revenue,
            gas_revenue,
            ngl_revenue,
            drip_condensate_revenue
        )
    )
),

/*
    Unpivot cash flow metrics
*/
cash_flow_metrics as (
    select
        economic_run_well_id,
        date,
        'cash_flow' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            before_income_tax_cash_flow,
            after_income_tax_cash_flow,
            first_discount_cash_flow,
            second_discount_cash_flow,
            first_discount_net_income,
            second_discount_net_income,
            first_discounted_capex,
            second_discounted_capex,
            net_income,
            net_profit
        )
    )
),

/*
    Unpivot expense metrics
*/
expense_metrics as (
    select
        economic_run_well_id,
        date,
        'expense' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            total_expense,
            total_fixed_expense,
            total_variable_expense,
            total_oil_variable_expense,
            total_gas_variable_expense,
            total_ngl_variable_expense,
            total_drip_condensate_variable_expense,
            monthly_well_cost,
            other_monthly_cost_1,
            other_monthly_cost_2
        )
    )
),

/*
    Unpivot production volume metrics
*/
production_volume_metrics as (
    select
        economic_run_well_id,
        date,
        'production_volume' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            gross_boe_sales_volume,
            gross_oil_sales_volume,
            gross_gas_sales_volume,
            gross_ngl_sales_volume,
            gross_drip_condensate_sales_volume,
            net_boe_sales_volume,
            net_oil_sales_volume,
            net_gas_sales_volume,
            net_ngl_sales_volume,
            net_drip_condensate_sales_volume,
            wi_boe_sales_volume,
            wi_oil_sales_volume,
            wi_gas_sales_volume,
            wi_ngl_sales_volume,
            wi_drip_condensate_sales_volume
        )
    )
),

/*
    Unpivot price metrics
*/
price_metrics as (
    select
        economic_run_well_id,
        date,
        'price' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            oil_price,
            gas_price,
            ngl_price,
            drip_condensate_price,
            input_oil_price,
            input_gas_price,
            input_ngl_price,
            input_drip_condensate_price
        )
    )
),

/*
    Unpivot capital expenditure metrics
*/
capex_metrics as (
    select
        economic_run_well_id,
        date,
        'capex' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            total_capex,
            total_intangible_capex,
            total_tangible_capex,
            total_gross_capex,
            total_drilling,
            total_completion,
            total_development,
            total_facilities,
            total_pad,
            total_pipelines,
            total_waterline,
            total_artificial_lift,
            total_workover,
            total_abandonment,
            total_salvage,
            total_leasehold,
            total_legal
        )
    )
),

/*
    Unpivot tax metrics
*/
tax_metrics as (
    select
        economic_run_well_id,
        date,
        'tax' as metric_category,
        metric_name,
        metric_value
    from source
    unpivot(
        metric_value for metric_name in (
            federal_income_tax,
            state_income_tax,
            taxable_income,
            ad_valorem_tax,
            total_production_tax,
            total_severance_tax,
            oil_severance_tax,
            gas_severance_tax,
            ngl_severance_tax,
            drip_condensate_severance_tax
        )
    )
),

/*
    Union all the metric categories
*/
unioned as (
    select * from revenue_metrics
    union all
    select * from cash_flow_metrics
    union all
    select * from expense_metrics
    union all
    select * from production_volume_metrics
    union all
    select * from price_metrics
    union all
    select * from capex_metrics
    union all
    select * from tax_metrics
),

/*
    Join back to source to get the metadata
*/
final as (
    select
        s.id,
        s._portable_extracted,
        s.econ_run,
        s.econ_run_date,
        s.date,
        s.combo_name,
        s.well_id,
        s.project,
        s.scenario,
        s.economic_run_well_id,
        u.metric_category,
        u.metric_name,
        u.metric_value
    from source s
    inner join unioned u
        on s.economic_run_well_id = u.economic_run_well_id
        and s.date = u.date
)

select * from final
order by
    project,
    scenario,
    econ_run,
    combo_name,
    well_id,
    date,
    metric_category,
    metric_name