{{ config(
    enabled=true,
    materialized='view'
) }}

--VOLUMES--
SELECT 
       --GL.Id
       CAST(C.CODE AS VARCHAR) AS "Company Code"
	  ,CAST(C.NAME AS VARCHAR) AS "Company Name"
      ,CAST(A.MAIN_ACCOUNT AS VARCHAR) AS "MainAccount"
	  ,CAST(A.SUB_ACCOUNT AS VARCHAR) AS "SubAccount"
	  ,CAST(A.MAIN_ACCOUNT AS VARCHAR) || '-' || CAST(A.SUB_ACCOUNT AS VARCHAR) AS "Combined Account"
	  ,CAST(A.MAIN_ACCOUNT AS VARCHAR) || '-' || CAST(A.SUB_ACCOUNT AS VARCHAR) || '-Vol' AS "Account Key"
      ,CAST(GL.IS_POSTED AS VARCHAR) AS "Posted"

      ,GL.JOURNAL_DATE AS "JE Date"

      ,GL.ACCRUAL_DATE AS "Accrual Date"

      ,CAST(GL.SOURCE_MODULE_CODE AS VARCHAR) AS "SourceModuleCode"
      ,CAST(GL.SOURCE_MODULE_NAME AS VARCHAR) AS "SourceModuleName"
      ,CAST(GL.SOURCE_MODULE AS VARCHAR) AS "SourceModule"
      ,CAST(V.Code AS VARCHAR) AS "Voucher Code"
      ,CAST(GL.DESCRIPTION AS VARCHAR) AS "Description"
      ,CAST(GL.PAYMENT_TYPE_CODE AS VARCHAR) AS "PaymentTypeCode"
	  ,CAST(GL.WELL_ID AS VARCHAR) AS "WellId"
      ,CAST(GL.REFERENCE AS VARCHAR) AS "Reference"
      ,CAST(GL.AFE_ID AS VARCHAR) AS "AfeId"
	  --,E.EntityType AS EntityType
	  ,CAST(E.CODE AS VARCHAR) AS "Enity Code"
	  ,CAST(E.Name AS VARCHAR) AS "Entity Name"
      ,CAST(GL.ENTITY_COMPANY_ID AS VARCHAR) AS "EntityCompanyId"
      ,CAST(GL.ENTITY_OWNER_ID AS VARCHAR) AS "EntityOwnerId"
      ,CAST (GL.ENTITY_VENDOR_ID AS VARCHAR) AS "EntityVendorId"
      ,CAST(GL.GROSS_VOLUME AS VARCHAR) AS "Gross Value"
      ,CAST(GL.NET_VOLUME AS VARCHAR) AS "Net Value"
      ,CAST(GL.LOCATION_TYPE AS VARCHAR) AS "LocationType"
      ,CAST(GL.AP_INVOICE_ID AS VARCHAR) AS "ApInvoiceId"
      ,CAST(GL.AR_INVOICE_ID AS VARCHAR) AS "ArInvoiceId"
      ,CAST(GL.AP_CHECK_ID AS VARCHAR) AS "ApCheckId"
      ,CAST(GL.CHECK_REVENUE_ID AS VARCHAR) AS "CheckRevenueId"
      ,CAST(GL.ENTRY_GROUP AS VARCHAR) AS "EntryGroup"
      ,CAST(GL.ORDINAL AS VARCHAR) AS "Ordinal"
      ,CAST(GL.IS_RECONCILED AS VARCHAR) AS "Reconciled"
  FROM {{ ref('stg_oda__gl') }} GL
  
  INNER JOIN {{ ref('stg_oda__account_v2') }} A
  ON CAST(GL.ACCOUNT_ID AS VARCHAR) = CAST(A.ID AS VARCHAR)
  
  INNER JOIN {{ ref('stg_oda__company_v2') }} C
  ON CAST(GL.COMPANY_ID AS VARCHAR) = CAST(C.ID AS VARCHAR)

  INNER JOIN {{ ref('stg_oda__voucher_v2') }} V
  ON CAST(GL.VOUCHER_ID AS VARCHAR) = CAST(V.ID AS VARCHAR)

  LEFT JOIN {{ ref('stg_oda__entity_v2') }} E
  ON CAST(GL.ENTITY_ID AS VARCHAR) = CAST(E.ID AS VARCHAR)



WHERE
	(GL.IS_POSTED = 1)
	AND
	(CAST(A.MAIN_ACCOUNT AS VARCHAR) IN ('701','702','703'))
	AND
	(CAST(A.SUB_ACCOUNT AS VARCHAR) IN ('1', '2', '3', '4', '5'))




UNION ALL

--VALUES--
SELECT 
       --GL.Id
       CAST(C.CODE AS VARCHAR) AS "Company Code"
	  ,CAST(C.NAME AS VARCHAR) AS "Company Name"
      ,CAST(A.MAIN_ACCOUNT AS VARCHAR) AS "MainAccount"
	  ,CAST(A.SUB_ACCOUNT AS VARCHAR) AS "SubAccount"
	  ,CAST(A.MAIN_ACCOUNT AS VARCHAR) || '-' || CAST(A.SUB_ACCOUNT AS VARCHAR) AS "Combined Account"
	  ,CAST(A.MAIN_ACCOUNT AS VARCHAR) || '-' || CAST(A.SUB_ACCOUNT AS VARCHAR) AS "Account Key"
      ,CAST(GL.IS_POSTED AS VARCHAR) AS "Posted"

      ,GL.JOURNAL_DATE AS "JE Date"

      ,GL.ACCRUAL_DATE AS "Accrual Date"

      ,CAST(GL.SOURCE_MODULE_CODE AS VARCHAR) AS "SourceModuleCode"
      ,CAST(GL.SOURCE_MODULE_NAME AS VARCHAR) AS "SourceModuleName"
      ,CAST(GL.SOURCE_MODULE AS VARCHAR) AS "SourceModule"
      ,CAST(V.Code AS VARCHAR) AS "Voucher Code"
      ,CAST(GL.DESCRIPTION AS VARCHAR) AS "Description"
      ,CAST(GL.PAYMENT_TYPE_CODE AS VARCHAR) AS "PaymentTypeCode"
	  ,CAST(GL.WELL_ID AS VARCHAR) AS "WellId"
      ,CAST(GL.REFERENCE AS VARCHAR) AS "Reference"
      ,CAST(GL.AFE_ID AS VARCHAR) AS "AfeId"
	  --,E.EntityType AS EntityType
	  ,CAST(E.CODE AS VARCHAR) AS "Enity Code"
	  ,CAST(E.Name AS VARCHAR) AS "Entity Name"
      ,CAST(GL.ENTITY_COMPANY_ID AS VARCHAR) AS "EntityCompanyId"
      ,CAST(GL.ENTITY_OWNER_ID AS VARCHAR) AS "EntityOwnerId"
      ,CAST (GL.ENTITY_VENDOR_ID AS VARCHAR) AS "EntityVendorId"
      ,CAST(GL.GROSS_VALUE AS VARCHAR) AS "Gross Value"
      ,CAST(GL.NET_VALUE AS VARCHAR) AS "Net Value"
      ,CAST(GL.LOCATION_TYPE AS VARCHAR) AS "LocationType"
      ,CAST(GL.AP_INVOICE_ID AS VARCHAR) AS "ApInvoiceId"
      ,CAST(GL.AR_INVOICE_ID AS VARCHAR) AS "ArInvoiceId"
      ,CAST(GL.AP_CHECK_ID AS VARCHAR) AS "ApCheckId"
      ,CAST(GL.CHECK_REVENUE_ID AS VARCHAR) AS "CheckRevenueId"
      ,CAST(GL.ENTRY_GROUP AS VARCHAR) AS "EntryGroup"
      ,CAST(GL.ORDINAL AS VARCHAR) AS "Ordinal"
      ,CAST(GL.IS_RECONCILED AS VARCHAR) AS "Reconciled"
  FROM {{ ref('stg_oda__gl') }} GL
  
  INNER JOIN {{ ref('stg_oda__account_v2') }} A
  ON CAST(GL.ACCOUNT_ID AS VARCHAR) = CAST(A.ID AS VARCHAR)
  
  INNER JOIN {{ ref('stg_oda__company_v2') }} C
  ON CAST(GL.COMPANY_ID AS VARCHAR) = CAST(C.ID AS VARCHAR)

  INNER JOIN {{ ref('stg_oda__voucher_v2') }} V
  ON CAST(GL.VOUCHER_ID AS VARCHAR) = CAST(V.ID AS VARCHAR)

  LEFT JOIN {{ ref('stg_oda__entity_v2') }} E
  ON CAST(GL.ENTITY_ID AS VARCHAR) = CAST(E.ID AS VARCHAR)



WHERE
	(GL.IS_POSTED = 1)
	AND
	(CAST( A.MAIN_ACCOUNT AS VARCHAR) IN ('310','311','312','313','314','315','316','317','328','701','702','703','840','850','860','870','704','900','715','901','807','903','830','806','802','318','935','704','705'))
  AND NOT "Company Code" IN (705, 801, 900)
