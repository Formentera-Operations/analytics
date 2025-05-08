{{
    config(
        materialized='view'
    )
}}

with source as (

    select * from {{ source('combo_curve', 'econ_run_monthly_export_results') }}

),

surrogate_key as (
    
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'ECONRUN',
            'WELL',
            'PROJECT',
            'SCENARIO',
            'COMBONAME'
        ]) }} as economic_run_well_id
    from source

),

converted as (

    select
        -- Metadata and Identifiers (keep as strings)
        _ID as id,
        _PORTABLE_EXTRACTED as extracted_at,
        ECONRUN as econ_run,
        ECON_RUN_DATE as econ_run_date,
        try_to_date(DATE) as date,
        COMBONAME as combo_name,
        WELL as well_id,
        PROJECT as project,
        SCENARIO as scenario,
        economic_run_well_id,

        -- Cash Flow Fields (convert to numeric)
        try_to_decimal(BEFOREINCOMETAXCASHFLOW, 38, 6) as before_income_tax_cash_flow,
        try_to_decimal(AFTERINCOMETAXCASHFLOW, 38, 6) as after_income_tax_cash_flow,
        try_to_decimal(FIRSTDISCOUNTCASHFLOW, 38, 6) as first_discount_cash_flow,
        try_to_decimal(SECONDDISCOUNTCASHFLOW, 38, 6) as second_discount_cash_flow,
        try_to_decimal(FIRSTDISCOUNTNETINCOME, 38, 6) as first_discount_net_income,
        try_to_decimal(SECONDDISCOUNTNETINCOME, 38, 6) as second_discount_net_income,
        try_to_decimal(FIRSTDISCOUNTEDCAPEX, 38, 6) as first_discounted_capex,
        try_to_decimal(SECONDDISCOUNTEDCAPEX, 38, 6) as second_discounted_capex,
        try_to_decimal(NETINCOME, 38, 6) as net_income,
        try_to_decimal(NETPROFIT, 38, 6) as net_profit,

        -- Discount Table Cash Flow Fields (convert to numeric)
        try_to_decimal(DISCOUNTTABLECASHFLOW1, 38, 6) as discount_table_cash_flow_1,
        try_to_decimal(DISCOUNTTABLECASHFLOW2, 38, 6) as discount_table_cash_flow_2,
        try_to_decimal(DISCOUNTTABLECASHFLOW3, 38, 6) as discount_table_cash_flow_3,
        try_to_decimal(DISCOUNTTABLECASHFLOW4, 38, 6) as discount_table_cash_flow_4,
        try_to_decimal(DISCOUNTTABLECASHFLOW5, 38, 6) as discount_table_cash_flow_5,
        try_to_decimal(DISCOUNTTABLECASHFLOW6, 38, 6) as discount_table_cash_flow_6,
        try_to_decimal(DISCOUNTTABLECASHFLOW7, 38, 6) as discount_table_cash_flow_7,
        try_to_decimal(DISCOUNTTABLECASHFLOW8, 38, 6) as discount_table_cash_flow_8,
        try_to_decimal(DISCOUNTTABLECASHFLOW9, 38, 6) as discount_table_cash_flow_9,
        try_to_decimal(DISCOUNTTABLECASHFLOW10, 38, 6) as discount_table_cash_flow_10,
        try_to_decimal(DISCOUNTTABLECASHFLOW11, 38, 6) as discount_table_cash_flow_11,
        try_to_decimal(DISCOUNTTABLECASHFLOW12, 38, 6) as discount_table_cash_flow_12,
        try_to_decimal(DISCOUNTTABLECASHFLOW13, 38, 6) as discount_table_cash_flow_13,
        try_to_decimal(DISCOUNTTABLECASHFLOW14, 38, 6) as discount_table_cash_flow_14,
        try_to_decimal(DISCOUNTTABLECASHFLOW15, 38, 6) as discount_table_cash_flow_15,
        try_to_decimal(DISCOUNTTABLECASHFLOW16, 38, 6) as discount_table_cash_flow_16,

        -- After-Tax Discount Table Cash Flow Fields (convert to numeric)
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW1, 38, 6) as afit_discount_table_cash_flow_1,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW2, 38, 6) as afit_discount_table_cash_flow_2,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW3, 38, 6) as afit_discount_table_cash_flow_3,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW4, 38, 6) as afit_discount_table_cash_flow_4,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW5, 38, 6) as afit_discount_table_cash_flow_5,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW6, 38, 6) as afit_discount_table_cash_flow_6,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW7, 38, 6) as afit_discount_table_cash_flow_7,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW8, 38, 6) as afit_discount_table_cash_flow_8,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW9, 38, 6) as afit_discount_table_cash_flow_9,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW10, 38, 6) as afit_discount_table_cash_flow_10,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW11, 38, 6) as afit_discount_table_cash_flow_11,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW12, 38, 6) as afit_discount_table_cash_flow_12,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW13, 38, 6) as afit_discount_table_cash_flow_13,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW14, 38, 6) as afit_discount_table_cash_flow_14,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW15, 38, 6) as afit_discount_table_cash_flow_15,
        try_to_decimal(AFITDISCOUNTTABLECASHFLOW16, 38, 6) as afit_discount_table_cash_flow_16,

        -- Revenue Fields (convert to numeric)
        try_to_decimal(TOTALREVENUE, 38, 6) as total_revenue,
        try_to_decimal(OILREVENUE, 38, 6) as oil_revenue,
        try_to_decimal(GASREVENUE, 38, 6) as gas_revenue,
        try_to_decimal(NGLREVENUE, 38, 6) as ngl_revenue,
        try_to_decimal(DRIPCONDENSATEREVENUE, 38, 6) as drip_condensate_revenue,

        -- Expense Fields (convert to numeric)
        try_to_decimal(TOTALEXPENSE, 38, 6) as total_expense,
        try_to_decimal(TOTALFIXEDEXPENSE, 38, 6) as total_fixed_expense,
        try_to_decimal(TOTALVARIABLEEXPENSE, 38, 6) as total_variable_expense,
        try_to_decimal(TOTALOILVARIABLEEXPENSE, 38, 6) as total_oil_variable_expense,
        try_to_decimal(TOTALGASVARIABLEEXPENSE, 38, 6) as total_gas_variable_expense,
        try_to_decimal(TOTALNGLVARIABLEEXPENSE, 38, 6) as total_ngl_variable_expense,
        try_to_decimal(TOTALDRIPCONDENSATEVARIABLEEXPENSE, 38, 6) as total_drip_condensate_variable_expense,
        try_to_decimal(MONTHLYWELLCOST, 38, 6) as monthly_well_cost,
        try_to_decimal(OTHERMONTHLYCOST_1, 38, 6) as other_monthly_cost_1,
        try_to_decimal(OTHERMONTHLYCOST_2, 38, 6) as other_monthly_cost_2,

        -- Oil Processing Expenses (convert to numeric)
        try_to_decimal(OILGATHERINGEXPENSE, 38, 6) as oil_gathering_expense,
        try_to_decimal(OILPROCESSINGEXPENSE, 38, 6) as oil_processing_expense,
        try_to_decimal(OILTRANSPORTATIONEXPENSE, 38, 6) as oil_transportation_expense,
        try_to_decimal(OILMARKETINGEXPENSE, 38, 6) as oil_marketing_expense,
        try_to_decimal(OILOTHEREXPENSE, 38, 6) as oil_other_expense,

        -- Gas Processing Expenses (convert to numeric)
        try_to_decimal(GASGATHERINGEXPENSE, 38, 6) as gas_gathering_expense,
        try_to_decimal(GASPROCESSINGEXPENSE, 38, 6) as gas_processing_expense,
        try_to_decimal(GASTRANSPORTATIONEXPENSE, 38, 6) as gas_transportation_expense,
        try_to_decimal(GASMARKETINGEXPENSE, 38, 6) as gas_marketing_expense,
        try_to_decimal(GASOTHEREXPENSE, 38, 6) as gas_other_expense,

        -- NGL Processing Expenses (convert to numeric)
        try_to_decimal(NGLGATHERINGEXPENSE, 38, 6) as ngl_gathering_expense,
        try_to_decimal(NGLPROCESSINGEXPENSE, 38, 6) as ngl_processing_expense,
        try_to_decimal(NGLTRANSPORTATIONEXPENSE, 38, 6) as ngl_transportation_expense,
        try_to_decimal(NGLMARKETINGEXPENSE, 38, 6) as ngl_marketing_expense,
        try_to_decimal(NGLOTHEREXPENSE, 38, 6) as ngl_other_expense,

        -- Drip Condensate Processing Expenses (convert to numeric)
        try_to_decimal(DRIPCONDENSATEGATHERINGEXPENSE, 38, 6) as drip_condensate_gathering_expense,
        try_to_decimal(DRIPCONDENSATEPROCESSINGEXPENSE, 38, 6) as drip_condensate_processing_expense,
        try_to_decimal(DRIPCONDENSATETRANSPORTATIONEXPENSE, 38, 6) as drip_condensate_transportation_expense,
        try_to_decimal(DRIPCONDENSATEMARKETINGEXPENSE, 38, 6) as drip_condensate_marketing_expense,
        try_to_decimal(DRIPCONDENSATEOTHEREXPENSE, 38, 6) as drip_condensate_other_expense,

        -- Water Expenses (convert to numeric)
        try_to_decimal(WATERDISPOSAL, 38, 6) as water_disposal,

        -- Volume Fields - Production Volumes (convert to numeric)
        try_to_decimal(GROSSBOESALESVOLUME, 38, 6) as gross_boe_sales_volume,
        try_to_decimal(GROSSBOEWELLHEADVOLUME, 38, 6) as gross_boe_wellhead_volume,
        try_to_decimal(GROSSOILSALESVOLUME, 38, 6) as gross_oil_sales_volume,
        try_to_decimal(GROSSOILWELLHEADVOLUME, 38, 6) as gross_oil_wellhead_volume,
        try_to_decimal(GROSSGASSALESVOLUME, 38, 6) as gross_gas_sales_volume,
        try_to_decimal(GROSSGASWELLHEADVOLUME, 38, 6) as gross_gas_wellhead_volume,
        try_to_decimal(GROSSNGLSALESVOLUME, 38, 6) as gross_ngl_sales_volume,
        try_to_decimal(GROSSDRIPCONDENSATESALESVOLUME, 38, 6) as gross_drip_condensate_sales_volume,
        try_to_decimal(GROSSMCFESALESVOLUME, 38, 6) as gross_mcfe_sales_volume,
        try_to_decimal(GROSSMCFEWELLHEADVOLUME, 38, 6) as gross_mcfe_wellhead_volume,
        try_to_decimal(GROSSWATERWELLHEADVOLUME, 38, 6) as gross_water_wellhead_volume,
        try_to_decimal(NETBOESALESVOLUME, 38, 6) as net_boe_sales_volume,
        try_to_decimal(NETOILSALESVOLUME, 38, 6) as net_oil_sales_volume,
        try_to_decimal(NETGASSALESVOLUME, 38, 6) as net_gas_sales_volume,
        try_to_decimal(NETNGLSALESVOLUME, 38, 6) as net_ngl_sales_volume,
        try_to_decimal(NETDRIPCONDENSATESALESVOLUME, 38, 6) as net_drip_condensate_sales_volume,
        try_to_decimal(NETMCFESALESVOLUME, 38, 6) as net_mcfe_sales_volume,

        -- Working Interest Volumes (convert to numeric)
        try_to_decimal(WIBOESALESVOLUME, 38, 6) as wi_boe_sales_volume,
        try_to_decimal(WIOILSALESVOLUME, 38, 6) as wi_oil_sales_volume,
        try_to_decimal(WIGASSALESVOLUME, 38, 6) as wi_gas_sales_volume,
        try_to_decimal(WINGLSALESVOLUME, 38, 6) as wi_ngl_sales_volume,
        try_to_decimal(WIDRIPCONDENSATESALESVOLUME, 38, 6) as wi_drip_condensate_sales_volume,
        try_to_decimal(WIMCFESALESVOLUME, 38, 6) as wi_mcfe_sales_volume,

        -- Price Fields (convert to numeric)
        try_to_decimal(OILPRICE, 38, 6) as oil_price,
        try_to_decimal(GASPRICE, 38, 6) as gas_price, 
        try_to_decimal(NGLPRICE, 38, 6) as ngl_price,
        try_to_decimal(DRIPCONDENSATEPRICE, 38, 6) as drip_condensate_price,
        try_to_decimal(INPUTOILPRICE, 38, 6) as input_oil_price,
        try_to_decimal(INPUTGASPRICE, 38, 6) as input_gas_price,
        try_to_decimal(INPUTNGLPRICE, 38, 6) as input_ngl_price,
        try_to_decimal(INPUTDRIPCONDENSATEPRICE, 38, 6) as input_drip_condensate_price,

        -- Differentials (convert to numeric)
        try_to_decimal(OILDIFFERENTIALS1, 38, 6) as oil_differentials_1,
        try_to_decimal(OILDIFFERENTIALS2, 38, 6) as oil_differentials_2,
        try_to_decimal(GASDIFFERENTIALS1, 38, 6) as gas_differentials_1,
        try_to_decimal(GASDIFFERENTIALS2, 38, 6) as gas_differentials_2,
        try_to_decimal(NGLDIFFERENTIALS1, 38, 6) as ngl_differentials_1,
        try_to_decimal(NGLDIFFERENTIALS2, 38, 6) as ngl_differentials_2,
        try_to_decimal(DRIPCONDENSATEDIFFERENTIALS1, 38, 6) as drip_condensate_differentials_1,
        try_to_decimal(DRIPCONDENSATEDIFFERENTIALS2, 38, 6) as drip_condensate_differentials_2,

        -- Capital Expenditure Fields (convert to numeric)
        try_to_decimal(TOTALCAPEX, 38, 6) as total_capex,
        try_to_decimal(TOTALINTANGIBLECAPEX, 38, 6) as total_intangible_capex,
        try_to_decimal(TOTALTANGIBLECAPEX, 38, 6) as total_tangible_capex,
        try_to_decimal(TOTALGROSSCAPEX, 38, 6) as total_gross_capex,

        -- Other Investment Fields (convert to numeric)
        try_to_decimal(TOTALOTHERINVESTMENT, 38, 6) as total_other_investment,
        try_to_decimal(INTANGIBLEOTHERINVESTMENT, 38, 6) as intangible_other_investment,
        try_to_decimal(TANGIBLEOTHERINVESTMENT, 38, 6) as tangible_other_investment,

        -- Fixed Expense Fields (convert to numeric)
        --try_to_decimal(FIXEDEXPENSE1, 38, 6) as fixed_expense_1,
        --try_to_decimal(FIXEDEXPENSE2, 38, 6) as fixed_expense_2,
        --try_to_decimal(FIXEDEXPENSE3, 38, 6) as fixed_expense_3,
        --try_to_decimal(FIXEDEXPENSE4, 38, 6) as fixed_expense_4,
        --try_to_decimal(FIXEDEXPENSE5, 38, 6) as fixed_expense_5,
        --try_to_decimal(FIXEDEXPENSE6, 38, 6) as fixed_expense_6,
        --try_to_decimal(FIXEDEXPENSE7, 38, 6) as fixed_expense_7,
        --try_to_decimal(FIXEDEXPENSE8, 38, 6) as fixed_expense_8,
        --try_to_decimal(FIXEDEXPENSE9, 38, 6) as fixed_expense_9,
        --try_to_decimal(FIXEDEXPENSE10, 38, 6) as fixed_expense_10,

        -- Development Capital (convert to numeric)
        try_to_decimal(TOTALDRILLING, 38, 6) as total_drilling,
        try_to_decimal(INTANGIBLEDRILLING, 38, 6) as intangible_drilling,
        try_to_decimal(TANGIBLEDRILLING, 38, 6) as tangible_drilling,
        try_to_decimal(TOTALCOMPLETION, 38, 6) as total_completion,
        try_to_decimal(INTANGIBLECOMPLETION, 38, 6) as intangible_completion,
        try_to_decimal(TANGIBLECOMPLETION, 38, 6) as tangible_completion,
        try_to_decimal(TOTALDEVELOPMENT, 38, 6) as total_development,
        try_to_decimal(INTANGIBLEDEVELOPMENT, 38, 6) as intangible_development,
        try_to_decimal(TANGIBLEDEVELOPMENT, 38, 6) as tangible_development,

        -- Facilities Capital (convert to numeric)
        try_to_decimal(TOTALFACILITIES, 38, 6) as total_facilities,
        try_to_decimal(INTANGIBLEFACILITIES, 38, 6) as intangible_facilities,
        try_to_decimal(TANGIBLEFACILITIES, 38, 6) as tangible_facilities,
        try_to_decimal(TOTALPAD, 38, 6) as total_pad,
        try_to_decimal(INTANGIBLEPAD, 38, 6) as intangible_pad,
        try_to_decimal(TANGIBLEPAD, 38, 6) as tangible_pad,
        try_to_decimal(TOTALPIPELINES, 38, 6) as total_pipelines,
        try_to_decimal(INTANGIBLEPIPELINES, 38, 6) as intangible_pipelines,
        try_to_decimal(TANGIBLEPIPELINES, 38, 6) as tangible_pipelines,
        try_to_decimal(TOTALWATERLINE, 38, 6) as total_waterline,
        try_to_decimal(INTANGIBLEWATERLINE, 38, 6) as intangible_waterline,
        try_to_decimal(TANGIBLEWATERLINE, 38, 6) as tangible_waterline,

        -- Other Capital Categories (convert to numeric)
        try_to_decimal(TOTALAPPRAISAL, 38, 6) as total_appraisal,
        try_to_decimal(INTANGIBLEAPPRAISAL, 38, 6) as intangible_appraisal,
        try_to_decimal(TANGIBLEAPPRAISAL, 38, 6) as tangible_appraisal,
        try_to_decimal(TOTALEXPLORATION, 38, 6) as total_exploration,
        try_to_decimal(INTANGIBLEEXPLORATION, 38, 6) as intangible_exploration,
        try_to_decimal(TANGIBLEEXPLORATION, 38, 6) as tangible_exploration,
        try_to_decimal(TOTALARTIFICIALLIFT, 38, 6) as total_artificial_lift,
        try_to_decimal(INTANGIBLEARTIFICIALLIFT, 38, 6) as intangible_artificial_lift,
        try_to_decimal(TANGIBLEARTIFICIALLIFT, 38, 6) as tangible_artificial_lift,
        try_to_decimal(TOTALWORKOVER, 38, 6) as total_workover,
        try_to_decimal(INTANGIBLEWORKOVER, 38, 6) as intangible_workover,
        try_to_decimal(TANGIBLEWORKOVER, 38, 6) as tangible_workover,
        try_to_decimal(TOTALABANDONMENT, 38, 6) as total_abandonment,
        try_to_decimal(INTANGIBLEABANDONMENT, 38, 6) as intangible_abandonment,
        try_to_decimal(TANGIBLEABANDONMENT, 38, 6) as tangible_abandonment,
        try_to_decimal(TOTALSALVAGE, 38, 6) as total_salvage,
        try_to_decimal(INTANGIBLESALVAGE, 38, 6) as intangible_salvage,
        try_to_decimal(TANGIBLESALVAGE, 38, 6) as tangible_salvage,
        try_to_decimal(TOTALLEASEHOLD, 38, 6) as total_leasehold,
        try_to_decimal(INTANGIBLELEASEHOLD, 38, 6) as intangible_leasehold,
        try_to_decimal(TANGIBLELEASEHOLD, 38, 6) as tangible_leasehold,
        try_to_decimal(TOTALLEGAL, 38, 6) as total_legal,
        try_to_decimal(INTANGIBLELEGAL, 38, 6) as intangible_legal,
        try_to_decimal(TANGIBLELEGAL, 38, 6) as tangible_legal,

        -- Tax Fields (convert to numeric)
        try_to_decimal(FEDERALINCOMETAX, 38, 6) as federal_income_tax,
        try_to_decimal(STATEINCOMETAX, 38, 6) as state_income_tax,
        try_to_decimal(TAXABLEINCOME, 38, 6) as taxable_income,
        try_to_decimal(ADVALOREMTAX, 38, 6) as ad_valorem_tax,
        try_to_decimal(TOTALPRODUCTIONTAX, 38, 6) as total_production_tax,
        try_to_decimal(TOTALSEVERANCETAX, 38, 6) as total_severance_tax,
        try_to_decimal(OILSEVERANCETAX, 38, 6) as oil_severance_tax,
        try_to_decimal(GASSEVERANCETAX, 38, 6) as gas_severance_tax,
        try_to_decimal(NGLSEVERANCETAX, 38, 6) as ngl_severance_tax,
        try_to_decimal(DRIPCONDENSATESEVERANCETAX, 38, 6) as drip_condensate_severance_tax,

        -- Accounting
        try_to_decimal(DEPRECIATION, 38, 6) as depreciation,

        -- Production Parameters (convert to numeric)
        try_to_decimal(GROSSWELLCOUNT, 38, 6) as gross_well_count,
        try_to_decimal(NRIWELLCOUNT, 38, 6) as nri_well_count,
        try_to_decimal(WIWELLCOUNT, 38, 6) as wi_well_count,
        try_to_decimal(NRIOIL, 38, 6) as nri_oil,
        try_to_decimal(NRIGAS, 38, 6) as nri_gas,
        try_to_decimal(NRINGL, 38, 6) as nri_ngl,
        try_to_decimal(NRIDRIPCONDENSATE, 38, 6) as nri_drip_condensate,
        try_to_decimal(WIOIL, 38, 6) as wi_oil,
        try_to_decimal(WIGAS, 38, 6) as wi_gas,
        try_to_decimal(WINGL, 38, 6) as wi_ngl,
        try_to_decimal(WIDRIPCONDENSATE, 38, 6) as wi_drip_condensate,
        try_to_decimal(LEASENRI, 38, 6) as lease_nri,

        -- Production Characteristics (convert to numeric)
        try_to_decimal(NGLYIELD, 38, 6) as ngl_yield,
        try_to_decimal(DRIPCONDENSATEYIELD, 38, 6) as drip_condensate_yield,
        try_to_decimal(OILLOSS, 38, 6) as oil_loss,
        try_to_decimal(OILSHRINKAGE, 38, 6) as oil_shrinkage,
        try_to_decimal(GASLOSS, 38, 6) as gas_loss,
        try_to_decimal(GASSHRINKAGE, 38, 6) as gas_shrinkage,
        try_to_decimal(GASFLARE, 38, 6) as gas_flare,

        -- Start Dates (convert to date)
        try_to_date(OILSTARTUSINGFORECASTDATE) as oil_start_using_forecast_date,
        try_to_date(GASSTARTUSINGFORECASTDATE) as gas_start_using_forecast_date,
        try_to_date(WATERSTARTUSINGFORECASTDATE) as water_start_using_forecast_date

        from surrogate_key

    )


select * from converted
order by
    project, 
    scenario, 
    econ_run, 
    combo_name, 
    well_id, 
    date asc