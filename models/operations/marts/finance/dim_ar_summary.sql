{{ config(
    enabled=true,
    materialized='table',
    tags=['marts', 'finance', 'dimension']
) }}

{#
    Dimension: AR Summary
    -- 
    

    
    Sources:
    - stg_oda__ar_invoice_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__voucher_v2
    - stg_oda__wells
    
#}

with ar_summary as 
    (
            SELECT
            c.Code                      AS company_code,
            c.Name                      AS company_name,
            i.Code                      AS invoice_code,
            i.invoice_date              AS invoice_date,
            i.invoice_amount            AS invoice_amount,
            i.Posted                    AS posted,
            i.is_overage_invoice        AS is_overage_invoice,
            i.accrual_date              AS accrual_date,
            i.include_in_accrual_report AS include_in_accrual_report,
            e.Code                      AS owner_code,
            v.Code                      AS voucher_code,
            Case 
			  When i.invoice_type_id = 0 Then 'ADVANCE'
			  When i.invoice_type_id = 1 Then 'CLOSEOUT'
			  When i.invoice_type_id = 2 Then 'GAS'
			  When i.invoice_type_id = 3 Then 'INTEREST'
			  When i.invoice_type_id = 4 Then 'JIB'
			  When i.invoice_type_id = 5 Then 'MISC'
			  When i.invoice_type_id = 6 Then 'REVENUE'
			  When i.invoice_type_id = 7 Then 'REVSTMT'
				                    End AS invoice_type,
            Case 
			  When i.statement_status_id   = 0 Then 'OPEN'
			  When i.statement_status_id   = 1 Then 'CLOSED'
			  When i.statement_status_id   = 2 Then 'NEVER'     
                                    End AS statement_status_id,
            w.Code                      AS well_code,
            i.advance_invoice_date      AS advance_date,
            i.create_date               AS create_date,
            i.update_date               AS update_date

        FROM       {{ ref('stg_oda__arinvoice_v2') }} i
        LEFT JOIN  {{ ref('stg_oda__company_v2') }}    c
            ON c.Id = i.company_id
        LEFT JOIN  {{ ref('stg_oda__owner_v2') }}      o
            ON o.Id = i.owner_id
        LEFT JOIN  {{ ref('stg_oda__entity_v2') }}     e
            ON e.Id = o.entity_id
        LEFT JOIN  {{ ref('stg_oda__voucher_v2') }}    v
            ON v.Id = i.voucher_id
        LEFT JOIN  {{ ref('stg_oda__wells') }}          w
            ON w.Id = i.well_id
    )

    select * from ar_summary