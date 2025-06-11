{{ config(
    materialized='table',
    indexes=[
        {'columns': ['account_id', 'journal_date']},
        {'columns': ['company_id', 'journal_date']},
        {'columns': ['journal_date']},
        {'columns': ['entity_id']},
        {'columns': ['well_id']},
        {'columns': ['afe_id']},
        {'columns': ['los_sort_order', 'journal_date']}
    ]
) }}

select 
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['id', 'company_id']) }} as gl_key,
    
    -- Natural keys
    id as gl_transaction_id,
    gl_identity,
    
    -- Foreign keys (dimensions)
    account_id,
    company_id,
    journal_date,
    entity_id,
    well_id,
    afe_id,
    
    -- Measures
    net_value,
    gross_value,
    net_volume,
    gross_volume,
    income_statement_amount,
    los_amount,
    net_value_absolute,
    
    -- Transaction attributes
    description,
    transaction_category,
    reference,
    source_module,
    entry_group,
    ordinal,
    
    -- Derived attributes
    fiscal_year,
    fiscal_quarter,
    fiscal_month,
    journal_month,
    journal_quarter,
    journal_year,
    amount_size_category,
    los_section,
    financial_classification,
    
    -- Status flags
    is_posted,
    is_generated_entry,
    is_reconciled,
    include_in_income_statement,
    include_in_los,
    
    -- LOS-specific attributes from account dimension
    los_sort_order,
    is_capital_account,
    
    -- Metadata
    create_date,
    record_insert_date,
    current_timestamp as fact_created_at
    
from {{ ref('int_gl_income_statement_prep') }}
where include_in_income_statement = true