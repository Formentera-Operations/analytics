{{ config(
    enable= true,
    materialized='view'
) }}

WITH rename as (
    SELECT 
       W."ID"
      ,CODE as "Code"
      ,CODE_SORT as "CodeSort"
      ,"NAME" as "Name"
      ,INACTIVE_DATE as "InactiveDate"
      ,LEGAL_DESCRIPTION as "LegalDescription"
      ,COUNTRY_NAME as "CountryName"
      ,STATE_CODE as "StateCode"
      ,STATE_NAME as "StateName"
      ,COUNTY_NAME as "CountyName"
      ,STRIPPER_WELL as "StripperWell"
      ,PROPERTY_REFERENCE_CODE as "PropertyReferenceCode"
	  ,CASE
			WHEN PROPERTY_REFERENCE_CODE = 'NON-OPERATED' THEN 'NON-OPERATED'
			ELSE 'OPERATED'
			END AS OP_REF
      ,API_NUMBER as "ApiNumber"
      ,OPERATING_GROUP_CODE as "OperatingGroupCode"
      ,OPERATING_GROUP_NAME as "OperatingGroupName"
      ,PRODUCTION_STATUS_NAME as "ProductionStatusName"
      ,N_ID as "NId"
      ,COST_CENTER_TYPE_CODE as "CostCenterTypeCode"
      ,COST_CENTER_TYPE_NAME as "CostCenterTypeName"
      ,OPERATOR_ID as "OperatorId"
      ,WELL_STATUS_TYPE_CODE as "WellStatusTypeCode"
      ,WELL_STATUS_TYPE_NAME as "WellStatusTypeName"
	    ,Max(Case When F.UserFieldName = 'UF-SEARCH KEY' Then F.UserFieldValueString End) As "SEARCHKEY"
	    ,Max(Case When F.UserFieldName = 'UF-PV FIELD' Then F.UserFieldValueString End) As "FIELD"
  FROM {{ ref('stg_oda__wells') }} W
  Left Join {{ ref('stg_oda__userfield') }} F
  On W.ID = F.Id
  GROUP BY
       W."ID"
      ,"CODE"
      ,"CODE_SORT"
      ,"NAME"
      ,"INACTIVE_DATE"
      ,"LEGAL_DESCRIPTION"
      ,"COUNTRY_NAME"
      ,"STATE_CODE"
      ,"STATE_NAME"
      ,"COUNTY_NAME"
      ,"STRIPPER_WELL"
      ,"PROPERTY_REFERENCE_CODE"
      ,"ApiNumber"
      ,"OPERATING_GROUP_CODE"
      ,"OPERATING_GROUP_NAME"
      ,"PRODUCTION_STATUS_NAME"
      ,"N_ID"
      ,"COST_CENTER_TYPE_CODE"
      ,"COST_CENTER_TYPE_NAME"
      ,"OPERATOR_ID"
      ,"WELL_STATUS_TYPE_CODE"
      ,"WELL_STATUS_TYPE_NAME"
      ,"OP_REF"
)

SELECT * FROM rename
  