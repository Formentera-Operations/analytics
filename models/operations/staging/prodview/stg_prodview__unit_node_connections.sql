{{ config(
    materialized='view',
    tags=['prodview', 'units', 'nodes', 'connections', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITNODEFLOWTO') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as flow_connection_id,
        idrecparent as outlet_id,
        idflownet as flow_network_id,
        
        -- Connection period
        dttmstart as connection_start_date,
        dttmend as connection_end_date,
        
        -- Inlet information
        idrecinlet as inlet_id,
        idrecinlettk as inlet_table,
        idrecinletunitcalc as inlet_unit_id_calculated,
        idrecinletunitcalctk as inlet_unit_table_calculated,
        
        -- Outlet information (calculated)
        idrecoutletcalc as outlet_id_calculated,
        idrecoutletcalctk as outlet_table_calculated,
        idrecoutletunitcalc as outlet_unit_id_calculated,
        idrecoutletunitcalctk as outlet_unit_table_calculated,
        
        -- Flow characteristics
        recircflow as is_recirculation_flow,
        com as comments,
        
        -- Sequence
        sysseq as sequence,
        
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