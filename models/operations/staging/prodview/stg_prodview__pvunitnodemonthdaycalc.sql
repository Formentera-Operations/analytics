{{
  config(
    materialized='view',
    alias='pvunitnodemonthdaycalc'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODEMONTHDAYCALC') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET,
        IDRECPARENT,
        IDREC,
        IDRECNODE,
        IDRECNODETK,
        
        -- Time period
        DTTM,
        YEAR,
        MONTH,
        DAYOFMONTH,
        
        -- Volume data with unit conversions (cubic meters to standard units)
        VOLHCLIQ / 0.158987294928 as VOLHCLIQ,
        case when VOLHCLIQ is not null then 'BBL' else null end as VOLHCLIQUNITLABEL,
        VOLHCLIQGASEQ / 28.316846592 as VOLHCLIQGASEQ,
        case when VOLHCLIQGASEQ is not null then 'MCF' else null end as VOLHCLIQGASEQUNITLABEL,
        VOLGAS / 28.316846592 as VOLGAS,
        case when VOLGAS is not null then 'MCF' else null end as VOLGASUNITLABEL,
        VOLWATER / 0.158987294928 as VOLWATER,
        case when VOLWATER is not null then 'BBL' else null end as VOLWATERUNITLABEL,
        VOLSAND / 0.158987294928 as VOLSAND,
        case when VOLSAND is not null then 'BBL' else null end as VOLSANDUNITLABEL,
        
        -- Heat content and heating value conversions
        HEAT / 1055055852.62 as HEAT,
        case when HEAT is not null then 'MMBTU' else null end as HEATUNITLABEL,
        FACTHEAT / 37258.9458078313 as FACTHEAT,
        case when FACTHEAT is not null then 'BTU/FT³' else null end as FACTHEATUNITLABEL,
        
        -- Facility reference
        IDRECFACILITY,
        IDRECFACILITYTK,
        
        -- System locking fields
        SYSLOCKMEUI,
        SYSLOCKCHILDRENUI,
        SYSLOCKME,
        SYSLOCKCHILDREN,
        SYSLOCKDATE,
        
        -- System audit fields
        SYSMODDATE,
        SYSMODUSER,
        SYSCREATEDATE,
        SYSCREATEUSER,
        SYSTAG,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as UPDATE_DATE,
        _FIVETRAN_DELETED as DELETED

    from source
)

select * from renamed