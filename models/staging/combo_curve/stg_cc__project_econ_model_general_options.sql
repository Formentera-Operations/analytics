with source as (

    select * from {{ source('combo_curve', 'project_econ_model_general_options') }}

),

renamed as (

    select
        -- ids
        id as project_econ_model_general_options_id,
        project as project_id,
        
        -- metadata
        name,
        econmodeltype as economic_model_type,
        copiedfrom as copied_from,
        _unique as is_unique,
        
        -- boe conversion parsing
        nullif(boeconversion:DRIPCONDENSATE,'')::float as boe_conversion_drip_condensate,
        nullif(boeconversion:DRYGAS,'')::float as boe_conversion_dry_gas,
        nullif(boeconversion:NGL,'')::float as boe_conversion_ngl,
        nullif(boeconversion:OIL,'')::float as boe_conversion_oil,
        nullif(boeconversion:WETGAS,'')::float as boe_conversion_wet_gas,
        
        -- discount table parsing
        nullif(discounttable:CASHACCRUALTIME,'')::string as discount_cash_accrual_time,
        nullif(discounttable:DISCOUNTMETHOD,'')::string as discount_method,
        -- Parse out individual discount rates
        nullif(discounttable:DISCOUNTS[0]:DISCOUNTTABLE,'')::float as discount_rate_1,
        nullif(discounttable:DISCOUNTS[1]:DISCOUNTTABLE,'')::float as discount_rate_2,
        nullif(discounttable:DISCOUNTS[2]:DISCOUNTTABLE,'')::float as discount_rate_3,
        nullif(discounttable:DISCOUNTS[3]:DISCOUNTTABLE,'')::float as discount_rate_4,
        nullif(discounttable:DISCOUNTS[4]:DISCOUNTTABLE,'')::float as discount_rate_5,
        nullif(discounttable:DISCOUNTS[5]:DISCOUNTTABLE,'')::float as discount_rate_6,
        nullif(discounttable:DISCOUNTS[6]:DISCOUNTTABLE,'')::float as discount_rate_7,
        nullif(discounttable:DISCOUNTS[7]:DISCOUNTTABLE,'')::float as discount_rate_8,
        nullif(discounttable:DISCOUNTS[8]:DISCOUNTTABLE,'')::float as discount_rate_9,
        nullif(discounttable:DISCOUNTS[9]:DISCOUNTTABLE,'')::float as discount_rate_10,
        nullif(discounttable:DISCOUNTS[10]:DISCOUNTTABLE,'')::float as discount_rate_11,
        nullif(discounttable:DISCOUNTS[11]:DISCOUNTTABLE,'')::float as discount_rate_12,
        nullif(discounttable:DISCOUNTS[12]:DISCOUNTTABLE,'')::float as discount_rate_13,
        nullif(discounttable:DISCOUNTS[13]:DISCOUNTTABLE,'')::float as discount_rate_14,
        nullif(discounttable:DISCOUNTS[14]:DISCOUNTTABLE,'')::float as discount_rate_15,
        nullif(discounttable:DISCOUNTS[15]:DISCOUNTTABLE,'')::float as discount_rate_16,
        nullif(discounttable:FIRSTDISCOUNT,'')::float as first_discount,
        nullif(discounttable:SECONDDISCOUNT,'')::float as second_discount,
        
        -- income tax parsing
        incometax:CARRYFORWARD::variant as income_tax_carry_forward,
        nullif(incometax:FEDERALINCOMETAX[0]:ENTIREWELLLIFE,'')::string as federal_income_tax_well_life,
        nullif(incometax:FEDERALINCOMETAX[0]:MULTIPLIER,'')::float as federal_income_tax_multiplier,
        incometax:FIFTEENDEPLETION::variant as income_tax_fifteen_depletion,
        nullif(incometax:STATEINCOMETAX[0]:ENTIREWELLLIFE,'')::string as state_income_tax_well_life,
        nullif(incometax:STATEINCOMETAX[0]:MULTIPLIER,'')::float as state_income_tax_multiplier,
        
        -- main options parsing
        nullif(mainoptions:AGGREGATIONDATE,'')::timestamp as aggregation_date,
        nullif(mainoptions:CURRENCY,'')::string as currency,
        nullif(mainoptions:FISCAL,'')::string as fiscal,
        nullif(mainoptions:INCOMETAX,'')::boolean as income_tax_enabled,
        nullif(mainoptions:PROJECTTYPE,'')::string as project_type,
        nullif(mainoptions:REPORTINGPERIOD,'')::string as reporting_period,
        
        -- reporting units parsing
        nullif(reportingunits:CASH,'')::string as cash_unit,
        nullif(reportingunits:CONDENSATEGASRATIO,'')::string as condensate_gas_ratio_unit,
        nullif(reportingunits:DRIPCONDENSATE,'')::string as drip_condensate_unit,
        nullif(reportingunits:DRIPCONDENSATEYIELD,'')::string as drip_condensate_yield_unit,
        nullif(reportingunits:GAS,'')::string as gas_unit,
        nullif(reportingunits:GOR,'')::string as gor_unit,
        nullif(reportingunits:NGL,'')::string as ngl_unit,
        nullif(reportingunits:NGLYIELD,'')::string as ngl_yield_unit,
        nullif(reportingunits:OIL,'')::string as oil_unit,
        nullif(reportingunits:PRESSURE,'')::string as pressure_unit,
        nullif(reportingunits:WATER,'')::string as water_unit,
        
        -- audit columns
        createdby as created_by,
        createdat as created_at,
        lastupdatedby as last_updated_by,
        updatedat as updated_at,
        _portable_extracted as portable_extracted_at

    from source

)

select * from renamed