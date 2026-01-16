{{ config(
    enabled=true,
    materialized='view'
) }}

with wells as (
    select *
      FROM {{ ref('stg_oda__wells') }}
),

field as (
    SELECT 
        "Id",
        "UserFieldName",
        "UserFieldValueString"
    FROM {{ ref('stg_oda__userfield') }}
    WHERE "UserFieldName" in ('UF-PV FIELD', 'UF-SEARCH KEY')
GROUP BY ALL
),

gl as (
    Select 
        gld.*
        ,loc_company.*
    FROM {{ ref('stg_oda__gl') }} gld
    LEFT OUTER JOIN {{ ref('stg_oda__company_v2') }} AS loc_company
        ON gld.company_id = loc_company.id
),

rename as (
    SELECT 
       W."ID"
      ,w.CODE as "Code"
      ,w.CODE_SORT as "CodeSort"
      ,w."NAME" as "Name"
      ,w.INACTIVE_DATE as "InactiveDate"
      ,w.LEGAL_DESCRIPTION as "LegalDescription"
      ,w.COUNTRY_NAME as "CountryName"
      ,w.STATE_CODE as "StateCode"
      ,w.STATE_NAME as "StateName"
      ,w.COUNTY_NAME as "CountyName"
      ,w.STRIPPER_WELL as "StripperWell"
      ,w.PROPERTY_REFERENCE_CODE as "PropertyReferenceCode"
	  ,CASE
			WHEN w.PROPERTY_REFERENCE_CODE = 'NON-OPERATED' THEN 'NON-OPERATED'
			ELSE 'OPERATED'
		END AS OP_REF
      ,CASE
			WHEN w.PROPERTY_REFERENCE_CODE = 'NON-OPERATED' THEN 0
			ELSE 1
		END AS OP_IS_OPERATED
      ,w.API_NUMBER as "ApiNumber"
      ,w.OPERATING_GROUP_CODE as "OperatingGroupCode"
      ,w.OPERATING_GROUP_NAME as "OperatingGroupName"
      ,w.PRODUCTION_STATUS_NAME as "ProductionStatusName"
      ,w.N_ID as "NId"
      ,w.COST_CENTER_TYPE_CODE as "CostCenterTypeCode"
      ,w.COST_CENTER_TYPE_NAME as "CostCenterTypeName"
      ,w.OPERATOR_ID as "OperatorId"
      ,w.WELL_STATUS_TYPE_CODE as "WellStatusTypeCode"
      ,w.WELL_STATUS_TYPE_NAME as "WellStatusTypeName"
	  ,max(Case When F."UserFieldName" = 'UF-SEARCH KEY' Then F."UserFieldValueString" End) As "SEARCHKEY"
	  ,max(Case When F."UserFieldName" = 'UF-PV FIELD' Then F."UserFieldValueString" End) As "FIELD"
      ,g.code as "CompanyCode"
      ,g.name as "CompanyName"
      ,g.full_name as company_full_name
      ,cast(w.create_date as date) as "Created Date"
      ,cast(w.update_date as date) as "Last Mod Date (UTC)"
  FROM wells W
  Left Join field F
  On W.ID = F."Id"
  left join gl g 
  ON w.id = g.well_id
  group by all
)

SELECT * FROM rename
group by all
  