{{ config(
    materialized='view',
    tags=['prodview', 'tanks', 'volumes', 'monthly', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKMONTHCALC') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as tank_calculation_id,
        idrecparent as parent_tank_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttmstart as period_start_date,
        dttmend as period_end_date,
        year as calculation_year,
        month as calculation_month,
        
        -- Opening inventory volumes (converted to US units)
        volstarttotal / 0.158987294928 as opening_total_volume_bbl,
        volstarthcliq / 0.158987294928 as opening_hcliq_volume_bbl,
        volstarthcliqgaseq / 28.316846592 as opening_gas_equivalent_hcliq_volume_mcf,
        volstartwater / 0.158987294928 as opening_water_volume_bbl,
        volstartsand / 0.158987294928 as opening_sand_volume_bbl,
        
        -- Opening quality measurements (converted to percentages)
        bswstart / 0.01 as opening_bsw_total_pct,
        sandcutstart / 0.01 as opening_sand_cut_total_pct,
        
        -- Closing inventory volumes (converted to US units)
        volendtotal / 0.158987294928 as closing_total_volume_bbl,
        volendhcliq / 0.158987294928 as closing_hcliq_volume_bbl,
        volendhcliqgaseq / 28.316846592 as closing_gas_equiv_hcliq_volume_mcf,
        volendwater / 0.158987294928 as closing_water_volume_bbl,
        volendsand / 0.158987294928 as closing_sand_volume_bbl,
        
        -- Closing quality measurements (converted to percentages)
        bswend / 0.01 as closing_bsw_total_pct,
        sandcutend / 0.01 as closing_sand_cut_total_pct,
        
        -- Change in inventory (converted to US units)
        volchgtotal / 0.158987294928 as change_in_total_volume_bbl,
        volchghcliq / 0.158987294928 as change_in_hcliq_volume_bbl,
        volchghcliqgaseq / 28.316846592 as change_in_gas_equivalent_hcliq_volume_mcf,
        volchgwater / 0.158987294928 as change_in_water_volume_bbl,
        volchgsand / 0.158987294928 as change_in_sand_volume_bbl,
        
        -- Facility and analysis references
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
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed