{{ config(
    materialized='view',
    tags=['prodview', 'nodes', 'configuration', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITNODE') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as node_id,
        idrecparent as parent_unit_id,
        idflownet as flow_network_id,
        
        -- Node configuration
        name as node_name,
        typ as node_type,
        dttmstart as start_date,
        dttmend as end_date,
        
        -- Fluid and component properties
        component as component_name,
        componentphase as component_phase,
        desfluid as designated_fluid,
        keepwhole as keep_whole,
        typfluidbaserestrict as fluid_base_restriction_type,
        
        -- Flow diagram and sorting
        sortflowdiag as flow_diagram_sort_order,
        
        -- Migration tracking
        keymigrationsource as migration_source_key,
        typmigrationsource as migration_source_type,
        
        -- External IDs and corrections
        otherid as other_id,
        correctionid1 as correction_id_1,
        correctiontyp1 as correction_type_1,
        correctionid2 as correction_id_2,
        correctiontyp2 as correction_type_2,
        
        -- Product and facility information
        facproductname as facility_product_name,
        usevirutalanalysis as use_virtual_analysis,
        
        -- Disposition configuration
        dispositionpoint as disposition_point,
        dispproductname as disposition_product_name,
        typdisp1 as disposition_type_1,
        typdisp2 as disposition_type_2,
        typdisphcliq as hcliq_disposition_type,
        dispida as disposition_id_a,
        dispidb as disposition_id_b,
        
        -- Purchaser information
        purchasername as purchaser_name,
        purchasercode1 as purchaser_code_1,
        purchasercode2 as purchaser_code_2,
        
        -- General configuration
        com as comments,
        dttmhide as hide_record_date,
        reportgroup as report_group,
        ingathered as is_in_gathered,
        
        -- User-defined fields
        usertxt1 as user_text_1,
        usertxt2 as user_text_2,
        usertxt3 as user_text_3,
        usernum1 as user_number_1,
        usernum2 as user_number_2,
        usernum3 as user_number_3,
        
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