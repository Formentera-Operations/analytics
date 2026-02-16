{{
    config(
        materialized='view',
        tags=['combo_curve', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('combo_curve', 'project_econ_model_general_options') }}
),

renamed as (
    select
        -- identifiers
        trim(id)::varchar as project_econ_model_general_options_id,
        trim(project)::varchar as project_id,

        -- descriptive fields
        trim(name)::varchar as name,
        trim(econmodeltype)::varchar as economic_model_type,
        trim(copiedfrom)::varchar as copied_from,

        -- flags
        _unique::boolean as is_unique,

        -- boe conversion
        nullif(boeconversion:DRIPCONDENSATE, '')::float as boe_conversion_drip_condensate,
        nullif(boeconversion:DRYGAS, '')::float as boe_conversion_dry_gas,
        nullif(boeconversion:NGL, '')::float as boe_conversion_ngl,
        nullif(boeconversion:OIL, '')::float as boe_conversion_oil,
        nullif(boeconversion:WETGAS, '')::float as boe_conversion_wet_gas,

        -- discount table
        nullif(discounttable:CASHACCRUALTIME, '')::varchar as discount_cash_accrual_time,
        nullif(discounttable:DISCOUNTMETHOD, '')::varchar as discount_method,
        nullif(discounttable:DISCOUNTS[0]:DISCOUNTTABLE, '')::float as discount_rate_1,
        nullif(discounttable:DISCOUNTS[1]:DISCOUNTTABLE, '')::float as discount_rate_2,
        nullif(discounttable:DISCOUNTS[2]:DISCOUNTTABLE, '')::float as discount_rate_3,
        nullif(discounttable:DISCOUNTS[3]:DISCOUNTTABLE, '')::float as discount_rate_4,
        nullif(discounttable:DISCOUNTS[4]:DISCOUNTTABLE, '')::float as discount_rate_5,
        nullif(discounttable:DISCOUNTS[5]:DISCOUNTTABLE, '')::float as discount_rate_6,
        nullif(discounttable:DISCOUNTS[6]:DISCOUNTTABLE, '')::float as discount_rate_7,
        nullif(discounttable:DISCOUNTS[7]:DISCOUNTTABLE, '')::float as discount_rate_8,
        nullif(discounttable:DISCOUNTS[8]:DISCOUNTTABLE, '')::float as discount_rate_9,
        nullif(discounttable:DISCOUNTS[9]:DISCOUNTTABLE, '')::float as discount_rate_10,
        nullif(discounttable:DISCOUNTS[10]:DISCOUNTTABLE, '')::float as discount_rate_11,
        nullif(discounttable:DISCOUNTS[11]:DISCOUNTTABLE, '')::float as discount_rate_12,
        nullif(discounttable:DISCOUNTS[12]:DISCOUNTTABLE, '')::float as discount_rate_13,
        nullif(discounttable:DISCOUNTS[13]:DISCOUNTTABLE, '')::float as discount_rate_14,
        nullif(discounttable:DISCOUNTS[14]:DISCOUNTTABLE, '')::float as discount_rate_15,
        nullif(discounttable:DISCOUNTS[15]:DISCOUNTTABLE, '')::float as discount_rate_16,
        nullif(discounttable:FIRSTDISCOUNT, '')::float as first_discount,
        nullif(discounttable:SECONDDISCOUNT, '')::float as second_discount,

        -- income tax
        incometax:CARRYFORWARD::variant as income_tax_carry_forward,
        nullif(incometax:FEDERALINCOMETAX[0]:ENTIREWELLLIFE, '')::varchar as federal_income_tax_well_life,
        nullif(incometax:FEDERALINCOMETAX[0]:MULTIPLIER, '')::float as federal_income_tax_multiplier,
        incometax:FIFTEENDEPLETION::variant as income_tax_fifteen_depletion,
        nullif(incometax:STATEINCOMETAX[0]:ENTIREWELLLIFE, '')::varchar as state_income_tax_well_life,
        nullif(incometax:STATEINCOMETAX[0]:MULTIPLIER, '')::float as state_income_tax_multiplier,

        -- main options
        nullif(mainoptions:AGGREGATIONDATE, '')::timestamp_ntz as aggregation_date,
        nullif(mainoptions:CURRENCY, '')::varchar as currency,
        nullif(mainoptions:FISCAL, '')::varchar as fiscal,
        nullif(mainoptions:INCOMETAX, '')::boolean as income_tax_enabled,
        nullif(mainoptions:PROJECTTYPE, '')::varchar as project_type,
        nullif(mainoptions:REPORTINGPERIOD, '')::varchar as reporting_period,

        -- reporting units
        nullif(reportingunits:CASH, '')::varchar as cash_unit,
        nullif(reportingunits:CONDENSATEGASRATIO, '')::varchar as condensate_gas_ratio_unit,
        nullif(reportingunits:DRIPCONDENSATE, '')::varchar as drip_condensate_unit,
        nullif(reportingunits:DRIPCONDENSATEYIELD, '')::varchar as drip_condensate_yield_unit,
        nullif(reportingunits:GAS, '')::varchar as gas_unit,
        nullif(reportingunits:GOR, '')::varchar as gor_unit,
        nullif(reportingunits:NGL, '')::varchar as ngl_unit,
        nullif(reportingunits:NGLYIELD, '')::varchar as ngl_yield_unit,
        nullif(reportingunits:OIL, '')::varchar as oil_unit,
        nullif(reportingunits:PRESSURE, '')::varchar as pressure_unit,
        nullif(reportingunits:WATER, '')::varchar as water_unit,

        -- dates
        createdby::varchar as created_by,
        createdat::timestamp_ntz as created_at,
        lastupdatedby::varchar as last_updated_by,
        updatedat::timestamp_ntz as updated_at,

        -- ingestion metadata
        _portable_extracted::timestamp_tz as _portable_extracted

    from source
),

filtered as (
    select *
    from renamed
    where project_econ_model_general_options_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['project_econ_model_general_options_id']) }} as econ_model_options_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        econ_model_options_sk,
        -- identifiers
        project_econ_model_general_options_id,
        project_id,
        -- descriptive fields
        name,
        economic_model_type,
        copied_from,
        -- flags
        is_unique,
        -- boe conversion
        boe_conversion_drip_condensate,
        boe_conversion_dry_gas,
        boe_conversion_ngl,
        boe_conversion_oil,
        boe_conversion_wet_gas,
        -- discount table
        discount_cash_accrual_time,
        discount_method,
        discount_rate_1,
        discount_rate_2,
        discount_rate_3,
        discount_rate_4,
        discount_rate_5,
        discount_rate_6,
        discount_rate_7,
        discount_rate_8,
        discount_rate_9,
        discount_rate_10,
        discount_rate_11,
        discount_rate_12,
        discount_rate_13,
        discount_rate_14,
        discount_rate_15,
        discount_rate_16,
        first_discount,
        second_discount,
        -- income tax
        income_tax_carry_forward,
        federal_income_tax_well_life,
        federal_income_tax_multiplier,
        income_tax_fifteen_depletion,
        state_income_tax_well_life,
        state_income_tax_multiplier,
        -- main options
        aggregation_date,
        currency,
        fiscal,
        income_tax_enabled,
        project_type,
        reporting_period,
        -- reporting units
        cash_unit,
        condensate_gas_ratio_unit,
        drip_condensate_unit,
        drip_condensate_yield_unit,
        gas_unit,
        gor_unit,
        ngl_unit,
        ngl_yield_unit,
        oil_unit,
        pressure_unit,
        water_unit,
        -- dates
        created_by,
        created_at,
        last_updated_by,
        updated_at,
        -- ingestion metadata
        _portable_extracted,
        -- dbt metadata
        _loaded_at
    from enhanced
)

select * from final
