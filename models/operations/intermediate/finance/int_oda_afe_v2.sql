{{ config(
    enable= true,
    materialized='view'
) }}

Select 
    ID AS "Id"
    , AFE_V2_IDENTITY AS "Afe_V2Identity"
    , N_ID  AS "NId"
    , CODE AS "Code"
    , CODE_SORT AS "CodeSort"
    , NAME AS "Name"
    , FULL_NAME AS "FullName"
    , FULL_DESCRIPTION AS "FullDescription"
    , AFE_TYPE_ID AS "AfeTypeId"
    , AFE_TYPE_CODE AS "AfeTypeCode"
    , AFE_TYPE_LABEL AS "AfeTypeLabel"
    , AFE_TYPE_FULL_NAME AS "AfeTypeFullName"
    , APPLICATION_TYPE_ID AS "ApplicationTypeId"
    , APPLICATION_TYPE_CODE AS "ApplicationTypeCode"
    , APPLICATION_TYPE_NAME AS "ApplicationTypeName"
    , BUDGET_USAGE_TYPE_ID AS "BudgetUsageTypeId"
    , BUDGET_USAGE_TYPE_CODE AS "BudgetUsageTypeCode"
    , BUDGET_USAGE_TYPE_NAME AS "BudgetUsageTypeName"
    , FIELD_ID AS "FieldId"
    , FIELD_CODE AS "FieldCode"
    , FIELD_CODE_SORT AS "FieldCodeSort"
    , FIELD_NAME AS "FieldName"
    , FIELD_DESCRIPTION AS "FieldDescription"
    , DEFAULT_COMPANY_CODE AS "DefaultCompanyCode"
    , DEFAULT_COMPANY_NAME AS "DefaultCompanyName"
    , DEFAULT_EXPENSE_DECK_CODE AS "DefaultExpenseDeckCode"
    , OPERATING_GROUP_ID AS "OperatingGroupId"
    , OPERATING_GROUP_CODE AS "OperatingGroupCode"
    , OPERATING_GROUP_NAME AS "OperatingGroupName"
    , WELL_ID AS "WellId"
    , WELL_CODE AS "WellCode"
    , WELL_NAME AS "WellName"
    , ACCOUNT_GROUP_NAME AS "AccountGroupName"
    , OPERATOR_REFERENCE AS "OperatorReference"
    , CLOSE_DATE AS "CloseDate"
    , COMPLETION_DATE AS "CompletionDate"
    , CREATE_DATE AS "CreateDate"
    , CREATE_EVENT_ID AS "CreateEventId"
    , UPDATE_DATE AS "UpdateDate"
    , UPDATE_EVENT_ID AS "UpdateEventId"
    , RECORD_INSERT_DATE AS "RecordInsertDate"
    , RECORD_UPDATE_DATE AS "RecordUpdateDate"
    , FLOW_PUBLISHED_AT
    , FLOW_DOCUMENT
from
{{ ref('stg_oda__afe_v2') }}