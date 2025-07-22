{{ config(
    materialized='view',
    tags=['prodview', 'flow_network', 'configuration', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVFLOWNETHEADER') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idflownet as flow_network_id,
        name as flow_network_name,
        typ as flow_network_type,
        
        -- Primary unit and facility references
        idrecunitprimary as primary_unit_id,
        idrecunitprimarytk as primary_unit_table,
        idrecfacilityprimary as primary_facility_id,
        idrecfacilityprimarytk as primary_facility_table,
        
        -- General information
        com as comments,
        
        -- Responsibility assignments
        idrecresp1 as primary_responsible_id,
        idrecresp1tk as primary_responsible_table,
        idrecresp2 as secondary_responsible_id,
        idrecresp2tk as secondary_responsible_table,
        
        -- Reporting configuration flags
        rptgatheredcalcs as report_gathered_calculations,
        rptallocations as report_allocations,
        rptdispositions as report_dispositions,
        rptcomponentdispositions as report_component_dispositions,
        rptnodecalculations as report_node_calculations,
        
        -- Operational settings
        trackdownholeinventory as track_downhole_inventory,
        
        -- Allocation and process dates
        dttmallocprocessbegan as allocation_process_began_date,
        dttmstart as start_date,
        dttmend as end_date,
        dttmlastallocprocess as last_allocation_process_date,
        userlastallocprocess as last_allocation_process_user,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usertxt4 as user_text_4,
        usertxt5 as user_text_5,
        
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
        
        -- Additional system fields
        sysmoddatedb as database_modified_at,
        sysmoduserdb as database_modified_by,
        syssecuritytyp as security_type,
        syslockdatemaster as master_lock_date,
        
        -- Fivetran fields
        _fivetran_synced as fivetran_synced_at

    from source_data
)

select * from renamed