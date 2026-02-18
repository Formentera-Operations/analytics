{{ config(
    enabled=true,
    materialized='view'
) }}

with wells as (
    select *
    from {{ ref('stg_oda__wells') }}
),

field as (
    select
        id,
        user_field_name,
        user_field_value_string
    from {{ ref('stg_oda__userfield') }}
    where user_field_name in ('UF-PV FIELD', 'UF-SEARCH KEY')
    group by all
),

gl as (
    select
        gld.*,
        loc_company.*
    from {{ ref('stg_oda__gl') }} gld
    left outer join {{ ref('stg_oda__company_v2') }} as loc_company
        on gld.company_id = loc_company.id
),

rename as (
    select
        W.ID, -- noqa: RF01
        w.CODE as "Code", -- noqa: RF01
        w.CODE_SORT as "CodeSort", -- noqa: RF01
        w.NAME as "Name", -- noqa: RF01
        w.INACTIVE_DATE as "InactiveDate", -- noqa: RF01
        w.LEGAL_DESCRIPTION as "LegalDescription", -- noqa: RF01
        w.COUNTRY_NAME as "CountryName", -- noqa: RF01
        w.STATE_CODE as "StateCode", -- noqa: RF01
        w.STATE_NAME as "StateName", -- noqa: RF01
        w.COUNTY_NAME as "CountyName", -- noqa: RF01
        w.IS_STRIPPER_WELL as "StripperWell", -- noqa: RF01
        w.PROPERTY_REFERENCE_CODE as "PropertyReferenceCode", -- noqa: RF01
        w.API_NUMBER as "ApiNumber", -- noqa: RF01
        w.OPERATING_GROUP_CODE as "OperatingGroupCode", -- noqa: RF01
        w.OPERATING_GROUP_NAME as "OperatingGroupName", -- noqa: RF01
        w.PRODUCTION_STATUS_NAME as "ProductionStatusName", -- noqa: RF01
        w.NID as "NId", -- noqa: RF01
        w.COST_CENTER_TYPE_CODE as "CostCenterTypeCode", -- noqa: RF01
        w.COST_CENTER_TYPE_NAME as "CostCenterTypeName", -- noqa: RF01
        w.OPERATOR_ID as "OperatorId", -- noqa: RF01
        w.WELL_STATUS_TYPE_CODE as "WellStatusTypeCode", -- noqa: RF01
        w.WELL_STATUS_TYPE_NAME as "WellStatusTypeName", -- noqa: RF01
        g.code as "CompanyCode",
        g.name as "CompanyName",
        g.full_name as company_full_name,
        cast(w.created_at as date) as "Created Date",
        cast(w.updated_at as date) as "Last Mod Date (UTC)",
        case
            when w.PROPERTY_REFERENCE_CODE = 'NON-OPERATED' then 'NON-OPERATED'
            else 'OPERATED'
        end as op_ref,
        case
            when w.PROPERTY_REFERENCE_CODE = 'NON-OPERATED' then 0
            else 1
        end as op_is_operated,
        max( -- noqa: RF01
            case when F.user_field_name = 'UF-SEARCH KEY' then F.user_field_value_string end -- noqa: RF01
        ) as searchkey,
        max( -- noqa: RF01
            case when F.user_field_name = 'UF-PV FIELD' then F.user_field_value_string end -- noqa: RF01
        ) as field
    from wells w
    left join field f
        on W.ID = F.id -- noqa: RF01
    left join gl g
        on w.id = g.well_id
    group by all
)

select * from rename
group by all
