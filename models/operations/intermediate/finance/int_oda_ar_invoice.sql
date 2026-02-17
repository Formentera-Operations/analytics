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
       SELECT
      ar.id                               AS invoice_id
    , ar.code                             AS invoice_number
    , ar.voucher_id
    , ar.invoice_amount
    , ar.invoice_date
    , ar.invoice_type_id
    , ar.description
    , ar.owner_id
    , ar.company_id
    , ar.well_id
    , vo.posted                           AS voucher_posted
    , vo.code                             AS voucher_code
    , c.code                              AS company_code
    , c.name                              AS company_name
    , e.code                              AS owner_code
    , e.name                              AS owner_name
    , w.code                              AS well_code
    , w.name                              AS well_name
    , COALESCE(w.hold_all_billing, 0)       AS hold_billing
    , CASE
          WHEN ar.invoice_type_id = 5 THEN 'Misc'
          WHEN ar.invoice_type_id = 0 THEN 'Adv'
          WHEN ar.invoice_type_id = 1 THEN 'Cls'
          ELSE 'JIB'
      END                                 AS invoice_type
    , CASE
          WHEN ar.invoice_type_id IN (5, 0, 1) THEN ar.description
          ELSE w.name
      END                                 AS invoice_description
    , CASE
          WHEN ar.invoice_type_id = 5 THEN 1
          WHEN ar.invoice_type_id = 0 THEN 2
          ELSE 1
      END                                 AS sort_order
        
        FROM {{ref('stg_oda__arinvoice_v2') }} AS ar

        INNER JOIN {{ref('stg_oda__voucher_v2')}} AS vo
            ON vo.id = ar.voucher_id

        INNER JOIN {{ref('stg_oda__company_v2')}} AS c
            ON c.id = ar.company_id

        INNER JOIN {{ref('stg_oda__owner_v2')}} AS o
            ON o.id = ar.owner_id

        INNER JOIN {{ref('stg_oda__entity_v2')}} AS e
            ON e.id = o.entity_id

        LEFT JOIN {{ref('stg_oda__wells')}} AS w
            ON w.id = ar.well_id



    )
        select * from ar_invoices

