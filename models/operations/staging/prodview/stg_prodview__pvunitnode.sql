
with source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODE') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        
        -- Node configuration
        NAME as name,
        TYP as type,
        DTTMSTART as dttm_start,
        DTTMEND as dttm_end,
        
        -- Fluid and component properties
        COMPONENT as component,
        COMPONENTPHASE as component_phase,
        DESFLUID as des_fluid,
        KEEPWHOLE as keep_whole,
        TYPFLUIDBASERESTRICT as type_fluid_base_restrict,
        
        -- Flow diagram and sorting
        SORTFLOWDIAG as sort_flow_diag,
        
        -- Migration tracking
        KEYMIGRATIONSOURCE as key_migration_source,
        TYPMIGRATIONSOURCE as type_migration_source,
        
        -- External IDs and corrections
        OTHERID as other_id,
        CORRECTIONID1 as correction_id_1,
        CORRECTIONTYP1 as correction_type_1,
        CORRECTIONID2 as correction_id_2,
        CORRECTIONTYP2 as correction_type_2,
        
        -- Product and facility information
        FACPRODUCTNAME as fac_product_name,
        USEVIRUTALANALYSIS as use_virtual_analysis,
        
        -- Disposition configuration
        DISPOSITIONPOINT as disposition_point,
        DISPPRODUCTNAME as disp_product_name,
        TYPDISP1 as type_disp_1,
        TYPDISP2 as type_disp_2,
        TYPDISPHCLIQ as type_disp_hc_liq,
        DISPIDA as disp_id_a,
        DISPIDB as disp_id_b,
        
        -- Purchaser information
        PURCHASERNAME as purchaser_name,
        PURCHASERCODE1 as purchaser_code_1,
        PURCHASERCODE2 as purchaser_code_2,
        
        -- General configuration
        COM as com,
        DTTMHIDE as dttm_hide,
        REPORTGROUP as report_group,
        INGATHERED as in_gathered,
        
        -- User-defined fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        USERNUM1 as user_num_1,
        USERNUM2 as user_num_2,
        USERNUM3 as user_num_3,
        
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