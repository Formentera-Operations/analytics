{{ config(
    materialized='table',
    indexes=[
        {'columns': ['account_code']},
        {'columns': ['financial_statement_type']},
        {'columns': ['los_line_item']},
        {'columns': ['los_sort_order']},
        {'columns': ['commodity_type', 'interest_type']}
    ]
) }}

select 
    account_id,
    account_code,
    account_name,
    account_full_name,
    main_account,
    sub_account,
    
    -- Core classifications
    financial_statement_type,
    los_line_item,
    income_statement_section,
    interest_type,
    commodity_type,
    expense_type,
    detailed_subcategory,
    
    -- LOS-specific attributes
    los_sort_order,
    los_value_type,
    is_los_account,
    is_capital_account,
    is_income_statement_account,
    
    -- Account behavior
    account_normally_debit,
    
    -- Hierarchy information
    type_code,
    type_name,
    subtype_code,
    subtype_name,
    
    current_timestamp as dim_created_at,
    current_timestamp as dim_updated_at
from {{ ref('int_accounts_classified') }}