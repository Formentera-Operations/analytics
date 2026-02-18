{{ config(
    enabled=true,
    materialized='view'
) }}

{#
    Dimension: Company AR Invoice Summary
    Core Invoice Data (JIB, Advances, Closeout, Misc Invoices)
    
    -- Layer 1: Base Invoice Model
    -- Purpose: Extract and standardize AR invoice data
    -- Dependencies: Base tables only
    
    Sources:
    - stg_oda__arinvoice_v2
    - stg_oda__company_v2
    - stg_oda__owner_v2
    - stg_oda__entity_v2
    - stg_oda__wells
#}

with ar_invoices as (
    select
        c.code as company_code,
        c.name as company_name,
        i.owner_id as owner_id,
        e.code as owner_code,
        e.name as owner_name,
        i.well_id as well_id,
        w.code as well_code,
        w.name as well_name,
        i.code as invoice_number,
        i.id as invoice_id,
        i.invoice_type_id as invoice_type_id,
        i.voucher_id as voucher_id,
        i.invoice_date as invoice_date,
        i.invoice_amount as total_invoice_amount,
        coalesce(w.is_hold_all_billing, false) as hold_billing,
        case
            when i.invoice_type_id = 5 then i.description
            when i.invoice_type_id = 0 then i.description
            when i.invoice_type_id = 1 then i.description
            else w.name
        end as invoice_description,
        case
            when i.invoice_type_id = 5 then 'Misc'
            when i.invoice_type_id = 0 then 'Adv'
            when i.invoice_type_id = 1 then 'Cls'
            else 'JIB'
        end as invoice_type,
        case
            when i.invoice_type_id = 5 then 1
            when i.invoice_type_id = 0 then 2
            else 1
        end as sort_order



    from {{ ref('stg_oda__arinvoice_v2') }} i

    inner join {{ ref('stg_oda__company_v2') }} c
        on i.company_id = c.id

    inner join {{ ref('stg_oda__owner_v2') }} o
        on i.owner_id = o.id

    inner join {{ ref('stg_oda__entity_v2') }} e
        on o.entity_id = e.Id

    -- LEFT JOIN {{ ref('stg_oda__voucher_v2') }} v
    -- ON v.id = i.voucher_id

    left join {{ ref('stg_oda__wells') }} w
        on i.well_id = w.id

    where i.is_posted
)

select * from ar_invoices
