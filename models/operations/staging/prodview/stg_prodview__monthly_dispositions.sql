{{ config(
    materialized='view',
    tags=['prodview', 'dispositions', 'monthly', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITDISPMONTH') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as disposition_id,
        idrecparent as parent_disposition_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttmstart as disposition_start_date,
        dttmend as disposition_end_date,
        year as disposition_year,
        month as disposition_month,
        
        -- Completion references
        idreccomp as completion_id,
        idreccomptk as completion_table,
        idreccompzone as reporting_contact_interval_id,
        idreccompzonetk as reporting_contact_interval_table,
        
        -- Outlet and disposition references
        idrecoutletsend as outlet_send_id,
        idrecoutletsendtk as outlet_send_table,
        idrecdispunitnode as disposition_unit_node_id,
        idrecdispunitnodetk as disposition_unit_node_table,
        idrecdispunit as disposition_unit_id,
        idrecdispunittk as disposition_unit_table,
        
        -- Total fluid volumes (converted to US units)
        volhcliq / 0.158987294928 as hcliq_volume_bbl,
        volhcliqgaseq / 28.316846592 as hcliq_gas_equivalent_mcf,
        volgas / 28.316846592 as gas_volume_mcf,
        volwater / 0.158987294928 as water_volume_bbl,
        volsand / 0.158987294928 as sand_volume_bbl,
        
        -- C1 (Methane) component volumes (converted to US units)
        volc1liq / 0.158987294928 as c1_liquid_volume_bbl,
        volc1gaseq / 28.316846592 as c1_gas_equivalent_mcf,
        volc1gas / 28.316846592 as c1_gas_volume_mcf,
        
        -- C2 (Ethane) component volumes (converted to US units)
        volc2liq / 0.158987294928 as c2_liquid_volume_bbl,
        volc2gaseq / 28.316846592 as c2_gas_equivalent_mcf,
        volc2gas / 28.316846592 as c2_gas_volume_mcf,
        
        -- C3 (Propane) component volumes (converted to US units)
        volc3liq / 0.158987294928 as c3_liquid_volume_bbl,
        volc3gaseq / 28.316846592 as c3_gas_equivalent_mcf,
        volc3gas / 28.316846592 as c3_gas_volume_mcf,
        
        -- iC4 (Iso-butane) component volumes (converted to US units)
        volic4liq / 0.158987294928 as ic4_liquid_volume_bbl,
        volic4gaseq / 28.316846592 as ic4_gas_equivalent_mcf,
        volic4gas / 28.316846592 as ic4_gas_volume_mcf,
        
        -- nC4 (Normal butane) component volumes (converted to US units)
        volnc4liq / 0.158987294928 as nc4_liquid_volume_bbl,
        volnc4gaseq / 28.316846592 as nc4_gas_equivalent_mcf,
        volnc4gas / 28.316846592 as nc4_gas_volume_mcf,
        
        -- iC5 (Iso-pentane) component volumes (converted to US units)
        volic5liq / 0.158987294928 as ic5_liquid_volume_bbl,
        volic5gaseq / 28.316846592 as ic5_gas_equivalent_mcf,
        volic5gas / 28.316846592 as ic5_gas_volume_mcf,
        
        -- nC5 (Normal pentane) component volumes (converted to US units)
        volnc5liq / 0.158987294928 as nc5_liquid_volume_bbl,
        volnc5gaseq / 28.316846592 as nc5_gas_equivalent_mcf,
        volnc5gas / 28.316846592 as nc5_gas_volume_mcf,
        
        -- C6 (Hexanes) component volumes (converted to US units)
        volc6liq / 0.158987294928 as c6_liquid_volume_bbl,
        volc6gaseq / 28.316846592 as c6_gas_equivalent_mcf,
        volc6gas / 28.316846592 as c6_gas_volume_mcf,
        
        -- C7+ (Heptanes plus) component volumes (converted to US units)
        volc7liq / 0.158987294928 as c7_liquid_volume_bbl,
        volc7gaseq / 28.316846592 as c7_gas_equivalent_mcf,
        volc7gas / 28.316846592 as c7_gas_volume_mcf,
        
        -- N2 (Nitrogen) component volumes (converted to US units)
        voln2liq / 0.158987294928 as n2_liquid_volume_bbl,
        voln2gaseq / 28.316846592 as n2_gas_equivalent_mcf,
        voln2gas / 28.316846592 as n2_gas_volume_mcf,
        
        -- CO2 (Carbon dioxide) component volumes (converted to US units)
        volco2liq / 0.158987294928 as co2_liquid_volume_bbl,
        volco2gaseq / 28.316846592 as co2_gas_equivalent_mcf,
        volco2gas / 28.316846592 as co2_gas_volume_mcf,
        
        -- H2S (Hydrogen sulfide) component volumes (converted to US units)
        volh2sliq / 0.158987294928 as h2s_liquid_volume_bbl,
        volh2sgaseq / 28.316846592 as h2s_gas_equivalent_mcf,
        volh2sgas / 28.316846592 as h2s_gas_volume_mcf,
        
        -- Other components volumes (converted to US units)
        volothercompliq / 0.158987294928 as other_components_liquid_volume_bbl,
        volothercompgaseq / 28.316846592 as other_components_gas_equivalent_mcf,
        volothercompgas / 28.316846592 as other_components_gas_volume_mcf,
        
        -- Heat content (converted to US units)
        heat / 1055055852.62 as heat_content_mmbtu,
        
        -- Calculation set reference
        idreccalcset as calc_settings_id,
        idreccalcsettk as calc_settings_table,
        
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