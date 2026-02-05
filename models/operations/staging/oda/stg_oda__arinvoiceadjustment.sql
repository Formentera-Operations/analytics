with source as (
    select * from {{ source('oda', 'ODA_BATCH_ODA_ARINVOICEADJUSTMENT') }}
),

renamed as (
        select
          -- ids
        ID                          AS id,
        ARINVOICEADJUSTMENTIDENTITY     AS ar_invoice_adjustment_identity,
        ACCOUNTID                   AS account_id,
        VOUCHERID                   AS voucher_id, 
        COMPANYID                   AS company_id, 
        ADJUSTMENTTYPEID            AS adjustment_type_id,
        OWNERID                     AS owner_id,
        
        --numerics
        ADJUSTMENTAMOUNT            AS adjustment_amount, 

        -- booleans
        INCLUDEINACCRUALREPORT      AS include_in_accrual_report,
        POSTED                      AS posted, 

        -- dates
        ACCRUALDATE                 AS accrual_date, 
        ADJUSTMENTDATE              AS adjustment_date, 

        -- strings
        DESCRIPTION                 AS description,
    
        -- timestamps and events
        CREATEDATE                  AS create_date,
        CREATEEVENTID               AS create_event_id,
        RECORDINSERTDATE            AS record_insert_date,
        RECORDUPDATEDATE            AS record_update_date,
        UPDATEDATE                  AS update_date,
        UPDATEEVENTID               AS update_event_id,

        -- metadata
       --_meta_op,
        FLOW_PUBLISHED_AT           AS flow_published_at,
        FLOW_DOCUMENT               AS flow_document


        from source
)

select * from renamed