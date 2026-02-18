-- noqa: disable=RF01
{{ config(
    enabled=true,
    materialized='view'
) }}

--VOLUMES--
select
    --GL.Id
    cast(C.CODE as varchar) as "Company Code",
    cast(C.NAME as varchar) as "Company Name",
    cast(A.MAIN_ACCOUNT as varchar) as "MainAccount",
    cast(A.SUB_ACCOUNT as varchar) as "SubAccount",
    cast(A.MAIN_ACCOUNT as varchar) || '-' || cast(A.SUB_ACCOUNT as varchar) as "Combined Account",
    cast(A.MAIN_ACCOUNT as varchar) || '-' || cast(A.SUB_ACCOUNT as varchar) || '-Vol' as "Account Key",
    cast(GL.IS_POSTED as varchar) as "Posted",

    GL.JOURNAL_DATE as "JE Date",

    GL.ACCRUAL_DATE as "Accrual Date",

    cast(GL.SOURCE_MODULE_CODE as varchar) as "SourceModuleCode",
    cast(GL.SOURCE_MODULE_NAME as varchar) as "SourceModuleName",
    cast(GL.SOURCE_MODULE as varchar) as "SourceModule",
    cast(V.Code as varchar) as "Voucher Code",
    cast(GL.DESCRIPTION as varchar) as "Description",
    cast(GL.PAYMENT_TYPE_CODE as varchar) as "PaymentTypeCode",
    cast(GL.WELL_ID as varchar) as "WellId",
    cast(GL.REFERENCE as varchar) as "Reference",
    cast(GL.AFE_ID as varchar) as "AfeId",
    --,E.EntityType AS EntityType
    cast(E.CODE as varchar) as "Enity Code",
    cast(E.Name as varchar) as "Entity Name",
    cast(GL.ENTITY_COMPANY_ID as varchar) as "EntityCompanyId",
    cast(GL.ENTITY_OWNER_ID as varchar) as "EntityOwnerId",
    cast(GL.ENTITY_VENDOR_ID as varchar) as "EntityVendorId",
    cast(GL.GROSS_VOLUME as varchar) as "Gross Value",
    cast(GL.NET_VOLUME as varchar) as "Net Value",
    cast(GL.LOCATION_TYPE as varchar) as "LocationType",
    cast(GL.AP_INVOICE_ID as varchar) as "ApInvoiceId",
    cast(GL.AR_INVOICE_ID as varchar) as "ArInvoiceId",
    cast(GL.AP_CHECK_ID as varchar) as "ApCheckId",
    cast(GL.CHECK_REVENUE_ID as varchar) as "CheckRevenueId",
    cast(GL.ENTRY_GROUP as varchar) as "EntryGroup",
    cast(GL.ORDINAL as varchar) as "Ordinal",
    cast(GL.IS_RECONCILED as varchar) as "Reconciled"
from {{ ref('stg_oda__gl') }} gl

inner join {{ ref('stg_oda__account_v2') }} a
    on cast(GL.ACCOUNT_ID as varchar) = cast(A.ID as varchar)

inner join {{ ref('stg_oda__company_v2') }} c
    on cast(GL.COMPANY_ID as varchar) = cast(C.ID as varchar)

inner join {{ ref('stg_oda__voucher_v2') }} v
    on cast(GL.VOUCHER_ID as varchar) = cast(V.ID as varchar)

left join {{ ref('stg_oda__entity_v2') }} e
    on cast(GL.ENTITY_ID as varchar) = cast(E.ID as varchar)



where
    (GL.IS_POSTED = 1)
    and
    (cast(A.MAIN_ACCOUNT as varchar) in ('701', '702', '703'))
    and
    (cast(A.SUB_ACCOUNT as varchar) in ('1', '2', '3', '4', '5'))




union all

--VALUES--
select
    --GL.Id
    cast(C.CODE as varchar) as "Company Code",
    cast(C.NAME as varchar) as "Company Name",
    cast(A.MAIN_ACCOUNT as varchar) as "MainAccount",
    cast(A.SUB_ACCOUNT as varchar) as "SubAccount",
    cast(A.MAIN_ACCOUNT as varchar) || '-' || cast(A.SUB_ACCOUNT as varchar) as "Combined Account",
    cast(A.MAIN_ACCOUNT as varchar) || '-' || cast(A.SUB_ACCOUNT as varchar) as "Account Key",
    cast(GL.IS_POSTED as varchar) as "Posted",

    GL.JOURNAL_DATE as "JE Date",

    GL.ACCRUAL_DATE as "Accrual Date",

    cast(GL.SOURCE_MODULE_CODE as varchar) as "SourceModuleCode",
    cast(GL.SOURCE_MODULE_NAME as varchar) as "SourceModuleName",
    cast(GL.SOURCE_MODULE as varchar) as "SourceModule",
    cast(V.Code as varchar) as "Voucher Code",
    cast(GL.DESCRIPTION as varchar) as "Description",
    cast(GL.PAYMENT_TYPE_CODE as varchar) as "PaymentTypeCode",
    cast(GL.WELL_ID as varchar) as "WellId",
    cast(GL.REFERENCE as varchar) as "Reference",
    cast(GL.AFE_ID as varchar) as "AfeId",
    --,E.EntityType AS EntityType
    cast(E.CODE as varchar) as "Enity Code",
    cast(E.Name as varchar) as "Entity Name",
    cast(GL.ENTITY_COMPANY_ID as varchar) as "EntityCompanyId",
    cast(GL.ENTITY_OWNER_ID as varchar) as "EntityOwnerId",
    cast(GL.ENTITY_VENDOR_ID as varchar) as "EntityVendorId",
    cast(GL.GROSS_VALUE as varchar) as "Gross Value",
    cast(GL.NET_VALUE as varchar) as "Net Value",
    cast(GL.LOCATION_TYPE as varchar) as "LocationType",
    cast(GL.AP_INVOICE_ID as varchar) as "ApInvoiceId",
    cast(GL.AR_INVOICE_ID as varchar) as "ArInvoiceId",
    cast(GL.AP_CHECK_ID as varchar) as "ApCheckId",
    cast(GL.CHECK_REVENUE_ID as varchar) as "CheckRevenueId",
    cast(GL.ENTRY_GROUP as varchar) as "EntryGroup",
    cast(GL.ORDINAL as varchar) as "Ordinal",
    cast(GL.IS_RECONCILED as varchar) as "Reconciled"
from {{ ref('stg_oda__gl') }} gl

inner join {{ ref('stg_oda__account_v2') }} a
    on cast(GL.ACCOUNT_ID as varchar) = cast(A.ID as varchar)

inner join {{ ref('stg_oda__company_v2') }} c
    on cast(GL.COMPANY_ID as varchar) = cast(C.ID as varchar)

inner join {{ ref('stg_oda__voucher_v2') }} v
    on cast(GL.VOUCHER_ID as varchar) = cast(V.ID as varchar)

left join {{ ref('stg_oda__entity_v2') }} e
    on cast(GL.ENTITY_ID as varchar) = cast(E.ID as varchar)



where
    (GL.IS_POSTED = 1)
    and
    (
        cast(A.MAIN_ACCOUNT as varchar) in (
            '310',
            '311',
            '312',
            '313',
            '314',
            '315',
            '316',
            '317',
            '328',
            '701',
            '702',
            '703',
            '840',
            '850',
            '860',
            '870',
            '704',
            '900',
            '715',
            '901',
            '807',
            '903',
            '830',
            '806',
            '802',
            '318',
            '935',
            '704',
            '705'
        )
    )
    and not "Company Code" in (705, 801, 900)
