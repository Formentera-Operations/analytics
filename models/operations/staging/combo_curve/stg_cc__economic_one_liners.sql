{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'econ_run_one_liners') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as economic_one_liner_id,
        trim(econrun)::varchar as economic_run_id,
        trim(project)::varchar as project_id,
        trim(scenario)::varchar as scenario_id,
        trim(well)::varchar as well_id,
        trim(comboname)::varchar as combo_name,

        -- descriptive fields
        trim(econgroup)::varchar as economic_group,

        -- category fields
        trim(econprmsreservescategory)::varchar as prms_reserves_category,
        trim(econprmsreservessubcategory)::varchar as prms_reserves_subcategory,
        trim(econprmsresourcesclass)::varchar as prms_resources_class,

        -- variant / json fields
        output as economic_output_raw,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where economic_one_liner_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['economic_one_liner_id']) }} as economic_one_liner_sk,
        {{ dbt_utils.generate_surrogate_key([
            'economic_run_id',
            'well_id',
            'project_id',
            'scenario_id',
            'combo_name'
        ]) }} as economic_run_well_id,

        -- identifiers
        economic_one_liner_id,
        economic_run_id,
        project_id,
        scenario_id,
        well_id,
        combo_name,

        -- descriptive fields
        economic_group,

        -- category fields
        prms_reserves_category,
        prms_reserves_subcategory,
        prms_resources_class,

        -- economic metrics
        get(economic_output_raw, 'after_tax_npv_10.0')::float as after_tax_npv_10,
        get(economic_output_raw, 'before_tax_npv_10.0')::float as before_tax_npv_10,
        get(economic_output_raw, 'breakeven_oil_price')::float as breakeven_oil_price,
        get(economic_output_raw, 'breakeven_oil_price_after_tax')::float as breakeven_oil_price_after_tax,
        get(economic_output_raw, 'capex')::float as capex,
        get(economic_output_raw, 'cum_oil')::float as cumulative_oil,
        get(economic_output_raw, 'cum_gas')::float as cumulative_gas,
        get(economic_output_raw, 'cum_boe')::float as cumulative_boe,
        get(economic_output_raw, 'cum_mcfe')::float as cumulative_mcfe,
        get(economic_output_raw, 'cum_ngl')::float as cumulative_ngl,
        get(economic_output_raw, 'eur_oil')::float as eur_oil,
        get(economic_output_raw, 'eur_gas')::float as eur_gas,
        get(economic_output_raw, 'eur_boe')::float as eur_boe,
        get(economic_output_raw, 'eur_mcfe')::float as eur_mcfe,
        get(economic_output_raw, 'eur_ngl')::float as eur_ngl,
        get(economic_output_raw, 'first_production')::date as first_production_date,
        get(economic_output_raw, 'gross_wells')::float as gross_wells_count,
        get(economic_output_raw, 'irr')::float as irr,
        get(economic_output_raw, 'irr_after_tax')::float as irr_after_tax,
        get(economic_output_raw, 'net_wells')::float as net_wells_count,
        get(economic_output_raw, 'payout_months')::float as payout_months,
        get(economic_output_raw, 'payout_months_after_tax')::float as payout_months_after_tax,
        get(economic_output_raw, 'total_expense')::float as total_expense,
        get(economic_output_raw, 'total_revenue')::float as total_revenue,
        get(economic_output_raw, 'wi_wells')::float as working_interest_wells_count,
        get(economic_output_raw, 'chosen_id')::string as chosen_id,
        get(economic_output_raw, 'basin')::string as basin,
        get(economic_output_raw, 'op_cat')::string as operating_category,
        get(economic_output_raw, 'shrunk_gas_btu')::float as shrunk_gas_btu,
        {{ transform_company_name("get(economic_output_raw, 'company_name')::string") }} as company_name,
        get(economic_output_raw, 'net_operating_income')::float as net_operating_income,
        get(economic_output_raw, 'before_income_tax_cash_flow')::float as before_income_tax_cash_flow,
        {{ transform_reserve_category("get(economic_output_raw, 'rsv_cat')::string") }} as reserves_category,

        -- reference values
        get(economic_output_raw, 'reference_after_tax_npv_10.0')::float as reference_after_tax_npv_10,
        get(economic_output_raw, 'reference_before_tax_npv_10.0')::float as reference_before_tax_npv_10,
        get(economic_output_raw, 'reference_breakeven_oil_price')::float as reference_breakeven_oil_price,
        get(economic_output_raw, 'reference_breakeven_oil_price_after_tax')::float
            as reference_breakeven_oil_price_after_tax,
        get(economic_output_raw, 'reference_capex')::float as reference_capex,
        get(economic_output_raw, 'reference_cum_oil')::float as reference_cumulative_oil,
        get(economic_output_raw, 'reference_cum_gas')::float as reference_cumulative_gas,
        get(economic_output_raw, 'reference_cum_boe')::float as reference_cumulative_boe,
        get(economic_output_raw, 'reference_cum_mcfe')::float as reference_cumulative_mcfe,
        get(economic_output_raw, 'reference_cum_ngl')::float as reference_cumulative_ngl,
        get(economic_output_raw, 'reference_eur_oil')::float as reference_eur_oil,
        get(economic_output_raw, 'reference_eur_gas')::float as reference_eur_gas,
        get(economic_output_raw, 'reference_eur_boe')::float as reference_eur_boe,
        get(economic_output_raw, 'reference_eur_mcfe')::float as reference_eur_mcfe,
        get(economic_output_raw, 'reference_eur_ngl')::float as reference_eur_ngl,
        get(economic_output_raw, 'reference_irr')::float as reference_irr,
        get(economic_output_raw, 'reference_irr_after_tax')::float as reference_irr_after_tax,
        get(economic_output_raw, 'reference_payout_months')::float as reference_payout_months,
        get(economic_output_raw, 'reference_payout_months_after_tax')::float as reference_payout_months_after_tax,
        get(economic_output_raw, 'reference_total_expense')::float as reference_total_expense,
        get(economic_output_raw, 'reference_total_revenue')::float as reference_total_revenue,

        -- variant / json fields
        economic_output_raw,

        -- ingestion metadata
        _portable_extracted,

        -- dbt metadata
        current_timestamp() as _loaded_at

    from filtered
),

final as (
    select
        economic_one_liner_sk,
        economic_run_well_id,
        -- identifiers
        economic_one_liner_id,
        economic_run_id,
        project_id,
        scenario_id,
        well_id,
        combo_name,
        -- descriptive fields
        economic_group,
        -- category fields
        prms_reserves_category,
        prms_reserves_subcategory,
        prms_resources_class,
        -- economic metrics
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
        first_production_date,
        gross_wells_count,
        irr,
        irr_after_tax,
        net_wells_count,
        payout_months,
        payout_months_after_tax,
        total_expense,
        total_revenue,
        working_interest_wells_count,
        chosen_id,
        basin,
        operating_category,
        shrunk_gas_btu,
        company_name,
        net_operating_income,
        before_income_tax_cash_flow,
        reserves_category,
        -- reference values
        reference_after_tax_npv_10,
        reference_before_tax_npv_10,
        reference_breakeven_oil_price,
        reference_breakeven_oil_price_after_tax,
        reference_capex,
        reference_cumulative_oil,
        reference_cumulative_gas,
        reference_cumulative_boe,
        reference_cumulative_mcfe,
        reference_cumulative_ngl,
        reference_eur_oil,
        reference_eur_gas,
        reference_eur_boe,
        reference_eur_mcfe,
        reference_eur_ngl,
        reference_irr,
        reference_irr_after_tax,
        reference_payout_months,
        reference_payout_months_after_tax,
        reference_total_expense,
        reference_total_revenue,
        -- variant / json fields
        economic_output_raw,
        -- ingestion metadata
        _portable_extracted,
        -- dbt metadata
        _loaded_at
    from enhanced
)

select * from final
