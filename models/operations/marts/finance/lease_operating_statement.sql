{{
    config(
        materialized='incremental',
        unique_key=['gl_id', 'account_key_formatted'],
        incremental_strategy='merge',
        cluster_by=['company_code', 'journal_date', 'transaction_type'],
        tags=['finance', 'gl', 'power_bi', 'analytics']
    )
}}

WITH gl_mart AS (
    SELECT *
    FROM {{ ref('general_ledger') }}
    WHERE posted = 'Y'  -- Only posted transactions
),

-- Define the account filters based on your Power BI query
filtered_accounts AS (
    SELECT
        *,
        CASE 
            WHEN transaction_type = 'Volume' THEN 
                CASE 
                    WHEN main_account IN (701, 702, 703) AND sub_account IN (1, 2, 3, 4, 5) THEN 1
                    ELSE 0
                END
            WHEN transaction_type = 'Value' THEN
                CASE
                    WHEN main_account IN (
                        310, 311, 312, 313, 314, 315, 316, 317, 
                        701, 702, 703, 840, 850, 860, 870, 704, 
                        900, 715, 901, 807, 903, 830, 806, 802, 
                        318, 935, 705
                    ) THEN 1
                    ELSE 0
                END
            ELSE 0
        END AS include_in_powerbi
    FROM gl_mart
)

SELECT
    -- Primary keys and metadata
    gl.gl_id,
    gl.created_date,
    gl.updated_date,
    gl.last_refresh_time,
    
    -- Company information (matching Power BI column names)
    gl.company_code AS company_code,
    gl.company_name AS company_name,
    
    -- Account information
    gl.main_account AS main_account,
    gl.sub_account AS sub_account,
    
    -- Combined account (matching Power BI logic)
    CAST(gl.main_account AS VARCHAR) || '-' || CAST(gl.sub_account AS VARCHAR) AS combined_account,
    
    -- Account Key with Volume/Value suffix (matching Power BI logic)
    CASE 
        WHEN gl.transaction_type = 'Volume' THEN 
            CAST(gl.main_account AS VARCHAR) || '-' || CAST(gl.sub_account AS VARCHAR) || '-Vol'
        ELSE 
            CAST(gl.main_account AS VARCHAR) || '-' || CAST(gl.sub_account AS VARCHAR)
    END AS account_key_formatted,
    
    -- Transaction type from your existing mart
    gl.transaction_type,
    gl.account_category,
    
    -- Posting information
    gl.posted,
    
    -- Date formatting (matching Power BI query format YYYY-MM)
    TO_CHAR(gl.journal_date, 'YYYY-MM') AS je_date,
    TO_CHAR(gl.accrual_date, 'YYYY-MM') AS accrual_date_formatted,
    
    -- Calendar attributes from your existing mart
    gl.fiscal_quarter,
    gl.fiscal_quarter_name,
    gl.fiscal_month_name,
    
    -- Source and reference information
    gl.source_module_code,
    gl.source_module_name,
    gl.source_module_code AS source_module,  -- For compatibility with Power BI query
    gl.voucher_code,
    gl.gl_description AS description,
    gl.reference_type AS payment_type_code,
    gl.reference,
    
    -- Well and AFE information
    gl.well_code AS well_id,  -- Matching Power BI field name
    gl.afe_code AS afe_id,    -- Matching Power BI field name
    
    -- Entity information
    CASE 
        WHEN gl.entity LIKE 'C:%' THEN 'Company'
        WHEN gl.entity LIKE 'O:%' THEN 'Owner'
        WHEN gl.entity LIKE 'P:%' THEN 'Purchaser'
        WHEN gl.entity LIKE 'V:%' THEN 'Vendor'
        ELSE NULL
    END AS entity_type,
    
    -- Clean entity code (remove prefix)
    CASE 
        WHEN gl.entity IS NOT NULL THEN TRIM(SUBSTRING(gl.entity, 4))
        ELSE NULL
    END AS entity_code,
    
    gl.entity_name,
    
    -- Placeholder entity IDs (add these to your mart model if available in source)
    --NULL AS entity_company_id,
    --NULL AS entity_owner_id,
    --NULL AS entity_vendor_id,
    
    -- Values - use appropriate field based on transaction type
    CASE 
        WHEN gl.transaction_type = 'Volume' THEN gl.gross_volume
        ELSE gl.gross_amount
    END AS gross_value,
    
    CASE 
        WHEN gl.transaction_type = 'Volume' THEN gl.net_volume
        ELSE gl.net_amount
    END AS net_value,
    
    -- Location information
    gl.location_type,
    gl.location_code,
    gl.location_name,
    
    -- Additional fields for Power BI compatibility
    --NULL AS ap_invoice_id,     -- Add to mart model if needed
    --NULL AS ar_invoice_id,     -- Add to mart model if needed
    --NULL AS ap_check_id,       -- Add to mart model if needed
    --NULL AS check_revenue_id,  -- Add to mart model if needed
    
    -- Entry metadata
    gl.entry_group,
    gl.entry_seq AS ordinal,
    gl.reconciled,
    
    -- Date fields for incremental logic and analysis
    gl.journal_date,
    gl.accrual_date,
    gl.cash_date,
    
    -- Additional calculated fields for enhanced analytics
    gl.gross_price_per_unit,
    gl.net_price_per_unit,
    
    -- Budget/planning information
    gl.revenue_deck_change_code,
    gl.expense_deck_change_code,
    
    -- Reconciliation status
    gl.reconciliation_type,
    gl.reconciled_trial

FROM filtered_accounts gl
WHERE gl.include_in_powerbi = 1  -- Only include accounts that match Power BI filters

{% if is_incremental() %}
    AND (
        gl.created_date > (SELECT MAX(created_date) FROM {{ this }})
        OR gl.updated_date > (SELECT MAX(updated_date) FROM {{ this }})
    )
{% endif %}