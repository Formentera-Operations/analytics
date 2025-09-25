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
	  --,Max(Case When F.UserFieldName = 'UF-SEARCH KEY' Then F.UserFieldValueString End) As "SEARCHKEY"
	  --,Max(Case When F.UserFieldName = 'UF-PV FIELD' Then F.UserFieldValueString End) As "FIELD"
  FROM {{ ref('stg_oda__wells') }} W
  --Left Join FO_RAW_DB."oda"."UserField" F
  --On W.ID = F.Id
)

SELECT * FROM rename
  GROUP BY
        "ID"
      ,"Code"
      ,"CodeSort"
      ,"Name"
      ,"InactiveDate"
      ,"LegalDescription"
      ,"CountryName"
      ,"StateCode"
      ,"StateName"
      ,"CountyName"
      ,"StripperWell"
      ,"PropertyReferenceCode"
      ,"ApiNumber"
      ,"OperatingGroupCode"
      ,"OperatingGroupName"
      ,"ProductionStatusName"
      ,"NId"
      ,"CostCenterTypeCode"
      ,"CostCenterTypeName"
      ,"OperatorId"
      ,"WellStatusTypeCode"
      ,"WellStatusTypeName"
      ,"OP_REF"