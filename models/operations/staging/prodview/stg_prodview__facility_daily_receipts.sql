{{ config(
    materialized='view',
    tags=['prodview', 'facilities', 'receipts_dispositions', 'daily', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVFACRECDISPCALC') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as facility_receipt_disposition_id,
        idflownet as flow_network_id,
        
        -- Date/Time information
        dttm as transaction_date,
        year as transaction_year,
        month as transaction_month,
        dayofmonth as day_of_month,
        
        -- Sending entity references
        idrecunitsend as sending_unit_id,
        idrecunitnodesend as sending_unit_node_id,
        idrecfacilitysend as sending_facility_id,
        idflownetsend as sending_flow_network_id,
        
        -- Receiving entity references
        idrecunitrec as receiving_unit_id,
        idrecunitnoderec as receiving_unit_node_id,
        idrecfacilityrec as receiving_facility_id,
        idflownetrec as receiving_flow_network_id,
        
        -- Volume measurements (converted to US units)
        volhcliq / 0.158987294928 as hcliq_volume_bbl,
        volgas / 28.316846592 as gas_volume_mcf,
        volgasplusgaseq / 28.316846592 as gas_plus_gas_equivalent_mcf,
        volwater / 0.158987294928 as water_volume_bbl,
        volsand / 0.158987294928 as sand_volume_bbl,
        
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