{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'finance', 'dimension']
) }}

{#
    Dimension: AR Summary
    -- AR Invoice Summary by Owner / Well
    
    Sources:
    - stg_oda__ar_invoice_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__voucher_v2
    - stg_oda__wells
    
#}

with ar_summary as (
    select
        c.Code as company_code,
        c.Name as company_name,
        i.Code as invoice_code,
        i.invoice_date as invoice_date,
        i.invoice_amount as invoice_amount,
        i.Posted as posted,
        i.is_overage_invoice as is_overage_invoice,
        i.accrual_date as accrual_date,
        i.include_in_accrual_report as include_in_accrual_report,
        e.code as owner_code,
        e.name as owner_name,
        o.is_hold_billing as hold_billing,
        v.code as voucher_code,
        w.Code as well_code,
        i.advance_invoice_date as advance_date,
        i.create_date as create_date,
        i.update_date as update_date,
        case
            when i.invoice_type_id = 0 then 'ADVANCE'
            when i.invoice_type_id = 1 then 'CLOSEOUT'
            when i.invoice_type_id = 2 then 'GAS'
            when i.invoice_type_id = 3 then 'INTEREST'
            when i.invoice_type_id = 4 then 'JIB'
            when i.invoice_type_id = 5 then 'MISC'
            when i.invoice_type_id = 6 then 'REVENUE'
            when i.invoice_type_id = 7 then 'REVSTMT'
        end as invoice_type,
        case
            when i.statement_status_id = 0 then 'OPEN'
            when i.statement_status_id = 1 then 'CLOSED'
            when i.statement_status_id = 2 then 'NEVER'
        end as statement_status_id

    from {{ ref('stg_oda__arinvoice_v2') }} i
    left join {{ ref('stg_oda__company_v2') }} c
        on i.company_id = c.Id
    left join {{ ref('stg_oda__owner_v2') }} o
        on i.owner_id = o.Id
    left join {{ ref('stg_oda__entity_v2') }} e
        on o.entity_id = e.Id
    left join {{ ref('stg_oda__voucher_v2') }} v
        on i.voucher_id = v.Id
    left join {{ ref('stg_oda__wells') }} w
        on i.well_id = w.Id
)

select * from ar_summary
