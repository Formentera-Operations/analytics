{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Summary
    
    Purpose: FP Company AR Invoice Summary
    Grain: AR Invoice Total (by Invoice/Voucher) per Owner per Well 
    
    Use cases:
    - Review AR Invoices & Status
    
    Sources:
    - stg_oda__arinvoice_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda_entity_v2
    - stg_oda_voucher_v2
    - stg_oda_wells
#}

    with arinvoice_summary as (
        select 
      -- =================================================================
        -- AR Summary
     -- =================================================================
        i.code,
        i.invoice_amount,
        v.Code                   AS voucher_code,
        Case         
                 When i.Posted  = '1' Then 'Y'
                 Else 'N'
                 End
                                 AS posted,   
        Case        
                 When i.is_overage_invoice = '1' Then 'Y'
                 Else 'N'
                 End
                                 AS is_overage_invoice,
        Case        
                 When i.include_in_accrual_report = '1' Then 'Y'
                 Else 'N'
                 End
                                 AS include_in_accrual_report,
        
         Case       
                 When i.invoice_type_id  = '0' Then 'ADVANCE'
                 When i.invoice_type_id  = '1' Then 'CLOSEOUT'
                 When i.invoice_type_id   = '2' Then 'GAS'
                 When i.invoice_type_id   = '3' Then 'INTEREST'
                 When i.invoice_type_id   = '4' Then 'JIB'
                 When i.invoice_type_id   = '5' Then 'MISC'
                 When i.invoice_type_id   = '6' Then 'REVENUE'
                 When i.invoice_type_id   = '7' Then 'REVSTMT'
                 Else ''
                 End
                                 AS invoice_type,
                       
        
        Case       
                 When i.statement_status_id = '0' Then 'OPEN'
                 When i.statement_status_id = '1' Then 'CLOSED'
                 When i.statement_status_id = '2' Then 'NEVER'      
                 Else ''
                 End
                                 AS statement_status,
        

    -- =================================================================
        --Company, Owner, Well Attributes
    -- =================================================================   
        c.Code                    AS company_code,
        e.Code                    AS owner_code,
        e.Name                    AS owner_name,
        w.Code                    AS well_code,
        w.Name                    AS well_name,    
    -- =================================================================
        --Accounting Dates
    -- =================================================================    
        i.invoice_date,
        i.invoice_date_key,
        i.accrual_date,
        i.advance_invoice_date,

     -- =================================================================
        --Change Dates
    -- =================================================================      
        i.create_date,
        i.update_date,
        i.record_insert_date,
        i.record_update_date
        
        FROM {{ref('stg_oda__arinvoice_v2') }} i

        LEFT JOIN {{ref('stg_oda__company_v2')}} c
        ON c.id = i.company_id
        
        LEFT JOIN {{ref('stg_oda__owner_v2')}} o
        ON o.id = i.owner_id

        LEFT JOIN {{ref('stg_oda__entity_v2')}} e
        ON e.Id = o.entity_id

        LEFT JOIN {{ref('stg_oda__voucher_v2')}} v
        ON v.id = i.voucher_id

        LEFT JOIN {{ref('stg_oda__wells')}} w
        ON w.id = i.well_id
        
        
    )
        select * from arinvoice_summary

