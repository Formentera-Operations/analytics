{{
    config(
        materialized='view',
        tags=['sharepoint', 'los', 'mapping']
    )
}}

with source as (

    select * from {{ source('sharepoint', 'los_mapping_master_oda_los_report_mapping_logic') }}

),

filled_hierarchy as (

    select
        _line,
        _fivetran_synced,
        key_sort,
        account,
        line,
        value_type,
        logic,
        name,
        report_header,
        
        -- Fill down the LINE number using last_value with ignore nulls
        last_value(line ignore nulls) over (
            order by _line 
            rows between unbounded preceding and current row
        ) as line_category_filled,
        
        -- Fill down the REPORT_HEADER using last_value with ignore nulls
        last_value(report_header ignore nulls) over (
            order by _line 
            rows between unbounded preceding and current row
        ) as report_header_filled

    from source

),

renamed as (

    select
        -- Primary key (using Fivetran's internal line number as unique identifier)
        _line as los_mapping_id,
        
        -- Audit columns
        _fivetran_synced as loaded_at,
        
        -- Hierarchical columns (filled down from category rows)
        line_category_filled as line_number,
        report_header_filled as report_header_category,
        
        -- Account mapping columns
        trim(account) as account_code,
        trim(key_sort) as key_sort_category,
        trim(name) as line_item_name,
        
        -- Logic and calculation columns
        trim(value_type) as value_type,
        trim(logic) as calculation_logic,
        
        -- Flags to identify row types
        case 
            when line is not null and account is null then true
            else false
        end as is_category_header,
        
        case
            when trim(logic) like 'TITLE ONLY%' then true
            else false
        end as is_title_only,
        
        case
            when trim(logic) like 'HIDDEN ROW%' or trim(logic) like '%HIDDEN ROW%' then true
            else false
        end as is_hidden_row,
        
        case
            when trim(logic) like 'BLANK%' then true
            else false
        end as is_blank_row,
        
        case
            when trim(logic) like 'CALC%' or trim(logic) like 'SUM%' then true
            else false
        end as is_calculated_row,
        
        -- Flag for subtraction in calculations
        case
            when trim(logic) = 'SUBTRACT' then true
            else false
        end as is_subtraction,
        
        -- Derived product type from report header
        case
            when report_header_filled ilike '%OIL%' then 'OIL'
            when report_header_filled ilike '%GAS%' then 'GAS'
            when report_header_filled ilike '%NGL%' or report_header_filled ilike '%LIQUID%' then 'NGL'
            when report_header_filled ilike '%BOE%' then 'BOE'
            else 'OTHER'
        end as product_type,
        
        -- Parse account number components (format: "701 / 1")
        case
            when account is not null and trim(account) like '%/%'
            then trim(split_part(trim(account), '/', 1))
            else null
        end as main_account,
        
        case
            when account is not null and trim(account) like '%/%'
            then trim(split_part(trim(account), '/', 2))
            else null
        end as sub_account,
        
        -- Unit of measure derived from value type and product
        case
            when value_type = 'NET QTY AMT' and product_type = 'OIL' then 'BARRELS'
            when value_type = 'NET QTY AMT' and product_type = 'GAS' then 'MCF'
            when value_type = 'NET QTY AMT' and product_type = 'NGL' then 'GALLONS'
            when value_type = 'NET VALUE AMT' then 'DOLLARS'
            else null
        end as unit_of_measure

    from filled_hierarchy

),

final as (

    select
        los_mapping_id,
        loaded_at,
        
        -- Hierarchical identifiers
        line_number,
        report_header_category,
        
        -- Account details
        account_code,
        main_account,
        sub_account,
        key_sort_category,
        line_item_name,
        
        -- Value and calculation details
        value_type,
        calculation_logic,
        unit_of_measure,
        
        -- Classification flags
        is_category_header,
        is_title_only,
        is_hidden_row,
        is_blank_row,
        is_calculated_row,
        is_subtraction,
        
        -- Derived attributes
        product_type
        
    from renamed
    
    -- Filter out pure title rows unless needed for reference
    where not (is_title_only and account_code is null)

)

select * from final
