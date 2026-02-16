{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'econ_run_monthly_export_results') }}
),

renamed as (
    select  -- noqa: ST06
        -- identifiers
        _ID::varchar as id,
        ECONRUN::varchar as econ_run_id,
        WELL::varchar as well_id,
        PROJECT::varchar as project_id,
        SCENARIO::varchar as scenario_id,
        COMBONAME::varchar as combo_name,

        -- dates
        ECON_RUN_DATE::varchar as econ_run_date,
        try_to_date(DATE) as date,

        -- cash flow
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

        -- discount table cash flow
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

        -- after-tax discount table cash flow
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

        -- revenue
        try_to_decimal(TOTALREVENUE, 38, 6) as total_revenue,
        try_to_decimal(OILREVENUE, 38, 6) as oil_revenue,
        try_to_decimal(GASREVENUE, 38, 6) as gas_revenue,
        try_to_decimal(NGLREVENUE, 38, 6) as ngl_revenue,
        try_to_decimal(DRIPCONDENSATEREVENUE, 38, 6) as drip_condensate_revenue,

        -- expenses (total)
        try_to_decimal(TOTALEXPENSE, 38, 6) as total_expense,
        try_to_decimal(TOTALFIXEDEXPENSE, 38, 6) as total_fixed_expense,
        try_to_decimal(TOTALVARIABLEEXPENSE, 38, 6) as total_variable_expense,
        try_to_decimal(TOTALOILVARIABLEEXPENSE, 38, 6) as total_oil_variable_expense,
        try_to_decimal(TOTALGASVARIABLEEXPENSE, 38, 6) as total_gas_variable_expense,
        try_to_decimal(TOTALNGLVARIABLEEXPENSE, 38, 6) as total_ngl_variable_expense,
        try_to_decimal(TOTALDRIPCONDENSATEVARIABLEEXPENSE, 38, 6) as total_drip_condensate_variable_expense,
        try_to_decimal(MONTHLYWELLCOST, 38, 6) as fixed_expense_1,
        try_to_decimal(OTHERMONTHLYCOST_1, 38, 6) as fixed_expense_2,
        try_to_decimal(OTHERMONTHLYCOST_2, 38, 6) as fixed_expense_3,
        try_to_decimal(OTHERMONTHLYCOST_3, 38, 6) as fixed_expense_4,
        try_to_decimal(OTHERMONTHLYCOST_4, 38, 6) as fixed_expense_5,
        try_to_decimal(OTHERMONTHLYCOST_5, 38, 6) as fixed_expense_6,
        try_to_decimal(OTHERMONTHLYCOST_6, 38, 6) as fixed_expense_7,
        try_to_decimal(OTHERMONTHLYCOST_7, 38, 6) as fixed_expense_8,
        try_to_decimal(OTHERMONTHLYCOST_8, 38, 6) as fixed_expense_9,

        -- expenses (oil processing)
        try_to_decimal(OILGATHERINGEXPENSE, 38, 6) as oil_gathering_expense,
        try_to_decimal(OILPROCESSINGEXPENSE, 38, 6) as oil_processing_expense,
        try_to_decimal(OILTRANSPORTATIONEXPENSE, 38, 6) as oil_transportation_expense,
        try_to_decimal(OILMARKETINGEXPENSE, 38, 6) as oil_marketing_expense,
        try_to_decimal(OILOTHEREXPENSE, 38, 6) as oil_other_expense,

        -- expenses (gas processing)
        try_to_decimal(GASGATHERINGEXPENSE, 38, 6) as gas_gathering_expense,
        try_to_decimal(GASPROCESSINGEXPENSE, 38, 6) as gas_processing_expense,
        try_to_decimal(GASTRANSPORTATIONEXPENSE, 38, 6) as gas_transportation_expense,
        try_to_decimal(GASMARKETINGEXPENSE, 38, 6) as gas_marketing_expense,
        try_to_decimal(GASOTHEREXPENSE, 38, 6) as gas_other_expense,

        -- expenses (ngl processing)
        try_to_decimal(NGLGATHERINGEXPENSE, 38, 6) as ngl_gathering_expense,
        try_to_decimal(NGLPROCESSINGEXPENSE, 38, 6) as ngl_processing_expense,
        try_to_decimal(NGLTRANSPORTATIONEXPENSE, 38, 6) as ngl_transportation_expense,
        try_to_decimal(NGLMARKETINGEXPENSE, 38, 6) as ngl_marketing_expense,
        try_to_decimal(NGLOTHEREXPENSE, 38, 6) as ngl_other_expense,

        -- expenses (drip condensate processing)
        try_to_decimal(DRIPCONDENSATEGATHERINGEXPENSE, 38, 6) as drip_condensate_gathering_expense,
        try_to_decimal(DRIPCONDENSATEPROCESSINGEXPENSE, 38, 6) as drip_condensate_processing_expense,
        try_to_decimal(DRIPCONDENSATETRANSPORTATIONEXPENSE, 38, 6) as drip_condensate_transportation_expense,
        try_to_decimal(DRIPCONDENSATEMARKETINGEXPENSE, 38, 6) as drip_condensate_marketing_expense,
        try_to_decimal(DRIPCONDENSATEOTHEREXPENSE, 38, 6) as drip_condensate_other_expense,

        -- water expenses
        try_to_decimal(WATERDISPOSAL, 38, 6) as water_disposal,

        -- volumes (gross)
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

        -- volumes (net)
        try_to_decimal(NETBOESALESVOLUME, 38, 6) as net_boe_sales_volume,
        try_to_decimal(NETOILSALESVOLUME, 38, 6) as net_oil_sales_volume,
        try_to_decimal(NETGASSALESVOLUME, 38, 6) as net_gas_sales_volume,
        try_to_decimal(NETNGLSALESVOLUME, 38, 6) as net_ngl_sales_volume,
        try_to_decimal(NETDRIPCONDENSATESALESVOLUME, 38, 6) as net_drip_condensate_sales_volume,
        try_to_decimal(NETMCFESALESVOLUME, 38, 6) as net_mcfe_sales_volume,

        -- volumes (working interest)
        try_to_decimal(WIBOESALESVOLUME, 38, 6) as wi_boe_sales_volume,
        try_to_decimal(WIOILSALESVOLUME, 38, 6) as wi_oil_sales_volume,
        try_to_decimal(WIGASSALESVOLUME, 38, 6) as wi_gas_sales_volume,
        try_to_decimal(WINGLSALESVOLUME, 38, 6) as wi_ngl_sales_volume,
        try_to_decimal(WIDRIPCONDENSATESALESVOLUME, 38, 6) as wi_drip_condensate_sales_volume,
        try_to_decimal(WIMCFESALESVOLUME, 38, 6) as wi_mcfe_sales_volume,

        -- prices
        try_to_decimal(OILPRICE, 38, 6) as oil_price,
        try_to_decimal(GASPRICE, 38, 6) as gas_price,
        try_to_decimal(NGLPRICE, 38, 6) as ngl_price,
        try_to_decimal(DRIPCONDENSATEPRICE, 38, 6) as drip_condensate_price,
        try_to_decimal(INPUTOILPRICE, 38, 6) as input_oil_price,
        try_to_decimal(INPUTGASPRICE, 38, 6) as input_gas_price,
        try_to_decimal(INPUTNGLPRICE, 38, 6) as input_ngl_price,
        try_to_decimal(INPUTDRIPCONDENSATEPRICE, 38, 6) as input_drip_condensate_price,

        -- differentials
        try_to_decimal(OILDIFFERENTIALS1, 38, 6) as oil_differentials_1,
        try_to_decimal(OILDIFFERENTIALS2, 38, 6) as oil_differentials_2,
        try_to_decimal(GASDIFFERENTIALS1, 38, 6) as gas_differentials_1,
        try_to_decimal(GASDIFFERENTIALS2, 38, 6) as gas_differentials_2,
        try_to_decimal(NGLDIFFERENTIALS1, 38, 6) as ngl_differentials_1,
        try_to_decimal(NGLDIFFERENTIALS2, 38, 6) as ngl_differentials_2,
        try_to_decimal(DRIPCONDENSATEDIFFERENTIALS1, 38, 6) as drip_condensate_differentials_1,
        try_to_decimal(DRIPCONDENSATEDIFFERENTIALS2, 38, 6) as drip_condensate_differentials_2,

        -- capital expenditure (total)
        try_to_decimal(TOTALCAPEX, 38, 6) as total_capex,
        try_to_decimal(TOTALINTANGIBLECAPEX, 38, 6) as total_intangible_capex,
        try_to_decimal(TOTALTANGIBLECAPEX, 38, 6) as total_tangible_capex,
        try_to_decimal(TOTALGROSSCAPEX, 38, 6) as total_gross_capex,

        -- capital expenditure (other investment)
        try_to_decimal(TOTALOTHERINVESTMENT, 38, 6) as total_other_investment,
        try_to_decimal(INTANGIBLEOTHERINVESTMENT, 38, 6) as intangible_other_investment,
        try_to_decimal(TANGIBLEOTHERINVESTMENT, 38, 6) as tangible_other_investment,

        -- capital expenditure (development)
        try_to_decimal(TOTALDRILLING, 38, 6) as total_drilling,
        try_to_decimal(INTANGIBLEDRILLING, 38, 6) as intangible_drilling,
        try_to_decimal(TANGIBLEDRILLING, 38, 6) as tangible_drilling,
        try_to_decimal(TOTALCOMPLETION, 38, 6) as total_completion,
        try_to_decimal(INTANGIBLECOMPLETION, 38, 6) as intangible_completion,
        try_to_decimal(TANGIBLECOMPLETION, 38, 6) as tangible_completion,
        try_to_decimal(TOTALDEVELOPMENT, 38, 6) as total_development,
        try_to_decimal(INTANGIBLEDEVELOPMENT, 38, 6) as intangible_development,
        try_to_decimal(TANGIBLEDEVELOPMENT, 38, 6) as tangible_development,

        -- capital expenditure (facilities)
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

        -- capital expenditure (other categories)
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

        -- taxes
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

        -- accounting
        try_to_decimal(DEPRECIATION, 38, 6) as depreciation,

        -- production parameters
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

        -- production characteristics
        try_to_decimal(NGLYIELD, 38, 6) as ngl_yield,
        try_to_decimal(DRIPCONDENSATEYIELD, 38, 6) as drip_condensate_yield,
        try_to_decimal(OILLOSS, 38, 6) as oil_loss,
        try_to_decimal(OILSHRINKAGE, 38, 6) as oil_shrinkage,
        try_to_decimal(GASLOSS, 38, 6) as gas_loss,
        try_to_decimal(GASSHRINKAGE, 38, 6) as gas_shrinkage,
        try_to_decimal(GASFLARE, 38, 6) as gas_flare,

        -- dates
        try_to_date(OILSTARTUSINGFORECASTDATE) as oil_start_using_forecast_date,
        try_to_date(GASSTARTUSINGFORECASTDATE) as gas_start_using_forecast_date,
        try_to_date(WATERSTARTUSINGFORECASTDATE) as water_start_using_forecast_date,

        -- ingestion metadata
        _PORTABLE_EXTRACTED::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as economic_run_monthly_sk,
        {{ dbt_utils.generate_surrogate_key([
            'econ_run_id',
            'well_id',
            'project_id',
            'scenario_id',
            'combo_name'
        ]) }} as economic_run_well_id,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        economic_run_monthly_sk,
        economic_run_well_id,
        -- identifiers
        id,
        econ_run_id,
        well_id,
        project_id,
        scenario_id,
        combo_name,
        -- dates
        econ_run_date,
        date,
        -- cash flow
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
        -- discount table cash flow
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
        -- after-tax discount table cash flow
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
        -- revenue
        total_revenue,
        oil_revenue,
        gas_revenue,
        ngl_revenue,
        drip_condensate_revenue,
        -- expenses
        total_expense,
        total_fixed_expense,
        total_variable_expense,
        total_oil_variable_expense,
        total_gas_variable_expense,
        total_ngl_variable_expense,
        total_drip_condensate_variable_expense,
        fixed_expense_1,
        fixed_expense_2,
        fixed_expense_3,
        fixed_expense_4,
        fixed_expense_5,
        fixed_expense_6,
        fixed_expense_7,
        fixed_expense_8,
        fixed_expense_9,
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
        -- volumes (gross)
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
        -- volumes (net)
        net_boe_sales_volume,
        net_oil_sales_volume,
        net_gas_sales_volume,
        net_ngl_sales_volume,
        net_drip_condensate_sales_volume,
        net_mcfe_sales_volume,
        -- volumes (working interest)
        wi_boe_sales_volume,
        wi_oil_sales_volume,
        wi_gas_sales_volume,
        wi_ngl_sales_volume,
        wi_drip_condensate_sales_volume,
        wi_mcfe_sales_volume,
        -- prices
        oil_price,
        gas_price,
        ngl_price,
        drip_condensate_price,
        input_oil_price,
        input_gas_price,
        input_ngl_price,
        input_drip_condensate_price,
        -- differentials
        oil_differentials_1,
        oil_differentials_2,
        gas_differentials_1,
        gas_differentials_2,
        ngl_differentials_1,
        ngl_differentials_2,
        drip_condensate_differentials_1,
        drip_condensate_differentials_2,
        -- capital expenditure
        total_capex,
        total_intangible_capex,
        total_tangible_capex,
        total_gross_capex,
        total_other_investment,
        intangible_other_investment,
        tangible_other_investment,
        total_drilling,
        intangible_drilling,
        tangible_drilling,
        total_completion,
        intangible_completion,
        tangible_completion,
        total_development,
        intangible_development,
        tangible_development,
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
        -- taxes
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
        -- accounting
        depreciation,
        -- production parameters
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
        -- production characteristics
        ngl_yield,
        drip_condensate_yield,
        oil_loss,
        oil_shrinkage,
        gas_loss,
        gas_shrinkage,
        gas_flare,
        -- dates
        oil_start_using_forecast_date,
        gas_start_using_forecast_date,
        water_start_using_forecast_date,
        -- ingestion metadata
        _portable_extracted,
        -- dbt metadata
        _loaded_at
    from enhanced
)

select * from final
