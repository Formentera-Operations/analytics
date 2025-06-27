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
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        IDRECNODE as id_rec_node,
        IDRECNODETK as id_rec_node_tk,
        
        -- Time period
        DTTM as dttm,
        YEAR as year,
        MONTH as month,
        DAYOFMONTH as day_of_month,
        
        -- Volume data with unit conversions (cubic meters to standard units)
        VOLHCLIQ / 0.158987294928 as vol_hc_liq,
        case when VOLHCLIQ is not null then 'BBL' else null end as vol_hc_liq_unit_label,
        VOLHCLIQGASEQ / 28.316846592 as vol_hc_liq_gas_eq,
        case when VOLHCLIQGASEQ is not null then 'MCF' else null end as vol_hc_liq_gas_eq_unit_label,
        VOLGAS / 28.316846592 as vol_gas,
        case when VOLGAS is not null then 'MCF' else null end as vol_gas_unit_label,
        VOLWATER / 0.158987294928 as vol_water,
        case when VOLWATER is not null then 'BBL' else null end as vol_water_unit_label,
        VOLSAND / 0.158987294928 as vol_sand,
        case when VOLSAND is not null then 'BBL' else null end as vol_sand_unit_label,
        
        -- Heat content and heating value conversions
        HEAT / 1055055852.62 as heat,
        case when HEAT is not null then 'MMBTU' else null end as heat_unit_label,
        FACTHEAT / 37258.9458078313 as fact_heat,
        case when FACTHEAT is not null then 'BTU/FT³' else null end as fact_heat_unit_label,
        
        -- Facility reference
        IDRECFACILITY as id_rec_facility,
        IDRECFACILITYTK as id_rec_facility_tk,
        
        -- System locking fields
        SYSLOCKMEUI as sys_lock_me_ui,
        SYSLOCKCHILDRENUI as sys_lock_children_ui,
        SYSLOCKME as sys_lock_me,
        SYSLOCKCHILDREN as sys_lock_children,
        SYSLOCKDATE as sys_lock_date,
        
        -- System audit fields
        SYSMODDATE as sys_mod_date,
        SYSMODUSER as sys_mod_user,
        SYSCREATEDATE as sys_create_date,
        SYSCREATEUSER as sys_create_user,
        SYSTAG as sys_tag,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed