{{
  config(
    materialized='view'
  )
}}

with source as (

    select * from {{ source('prodview', 'PVT_PVUNITTANKMONTHDAYCALC') }}

),

renamed as (

    select
        -- Primary identifiers
        idrec as tank_record_id,
        idrecparent as parent_record_id,
        idflownet as flow_network_id,
        
        -- Tank reference
        idrectank as tank_id,
        idrectanktk as tank_table,
        
        -- Date information
        dttm as tank_date,
        year as tank_year,
        month as tank_month,
        dayofmonth as day_of_month,
        
        -- Opening inventory volumes (converted to imperial units)
        volstarttotal / 0.158987294928 as opening_total_volume_bbl,
        volstarthcliq / 0.158987294928 as opening_oil_condensate_volume_bbl,
        volstarthcliqgaseq / 28.316846592 as opening_gas_equivalent_oil_cond_volume_mcf,
        volstartwater / 0.158987294928 as opening_water_volume_bbl,
        volstartsand / 0.158987294928 as opening_sand_volume_bbl,
        
        -- Opening inventory percentages (converted to percentage)
        bswstart / 0.01 as opening_bsw_total_pct,
        sandcutstart / 0.01 as opening_sand_cut_total_pct,
        
        -- Closing inventory volumes (converted to imperial units)
        volendtotal / 0.158987294928 as closing_total_volume_bbl,
        volendhcliq / 0.158987294928 as closing_oil_condensate_volume_bbl,
        volendhcliqgaseq / 28.316846592 as closing_gas_equivalent_oil_cond_volume_mcf,
        volendwater / 0.158987294928 as closing_water_volume_bbl,
        volendsand / 0.158987294928 as closing_sand_volume_bbl,
        
        -- Closing inventory percentages (converted to percentage)
        bswend / 0.01 as closing_bsw_total_pct,
        sandcutend / 0.01 as closing_sand_cut_total_pct,
        
        -- Change in inventory volumes (converted to imperial units)
        volchgtotal / 0.158987294928 as change_total_volume_bbl,
        volchghcliq / 0.158987294928 as change_oil_condensate_volume_bbl,
        volchghcliqgaseq / 28.316846592 as change_gas_equivalent_oil_cond_volume_mcf,
        volchgwater / 0.158987294928 as change_water_volume_bbl,
        volchgsand / 0.158987294928 as change_sand_volume_bbl,
        
        -- Reference fields
        idrecfacility as current_facility_id,
        idrecfacilitytk as current_facility_table,
        idrechcliqanalysis as hc_liquid_analysis_id,
        idrechcliqanalysistk as hc_liquid_analysis_table,
        
        -- System fields
        syscreatedate as created_at,
        syscreateuser as created_by,
        sysmoddate as modified_at,
        sysmoduser as modified_by,
        systag as system_tag,
        syslockdate as system_lock_date,
        syslockme as system_lock_me,
        syslockchildren as system_lock_children,
        syslockmeui as system_lock_me_ui,
        syslockchildrenui as system_lock_children_ui,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at,
        _fivetran_deleted as is_deleted

    from source

)

select * from renamed