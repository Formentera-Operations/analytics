{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'dimension']
    )
}}

{#
    Dimension: Accounting - Accounts Payable Check Register
    
    
    Sources:
    - dim_ap_check_register
    - dim_revenue_check_register
#}

select 
    'AP Check Register',
    *
    from {{ ref('dim_ap_check_register') }}

    union all

select
    'Revenue Check Register',
    *
    from {{ ref('dim_revenue_check_register') }} 