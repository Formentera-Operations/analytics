{{
  config(
    materialized='view',
    alias='pvunitmeterliquid'
  )
}}

with source as (
    select * from {{ source('prodview', 'PVT_PVUNITMETERLIQUID') }}
),

renamed as (
    select
        -- Primary identifiers
        IDFLOWNET as id_flow_net,
        IDRECPARENT as id_rec_parent,
        IDREC as id_rec,
        
        -- Meter configuration
        NAME as name,
        ENTRYSOURCE as entry_source,
        TYP as type,
        TYPRECORDING as type_recording,
        UOMVOL as uom_vol,
        PRODUCTNAME as product_name,
        
        -- Meter settings and calibration
        REZEROSTART as rezero_start,
        READINGROLLOVER as reading_rollover,
        ESTMISSINGDAY as est_missing_day,
        
        -- Initial BSW (Basic Sediment and Water) with unit conversion (decimal to percentage)
        INITIALBSW / 0.01 as initial_bsw,
        case when INITIALBSW is not null then '%' else null end as initial_bsw_unit_label,
        
        -- Identification numbers
        SERIALNUM as serial_num,
        ENGINEERINGID as engineering_id,
        REGULATORYID as regulatory_id,
        OTHERID as other_id,
        
        -- Location and references
        LOCPROVTAP as loc_prov_tap,
        IDRECUNITNODECALC as id_rec_unit_node_calc,
        IDRECUNITNODECALCTK as id_rec_unit_node_calc_tk,
        IDRECUNITDATAENTRYOR as id_rec_unit_data_entry_or,
        IDRECUNITDATAENTRYORTK as id_rec_unit_data_entry_or_tk,
        
        -- Import/Export tracking
        IMPORTID1 as import_id_1,
        IMPORTTYP1 as import_type_1,
        IMPORTID2 as import_id_2,
        IMPORTTYP2 as import_type_2,
        EXPORTID1 as export_id_1,
        EXPORTTYP1 as export_type_1,
        EXPORTID2 as export_id_2,
        EXPORTTYP2 as export_type_2,
        
        -- Operational settings
        ENTRYREQPERIOD as entry_req_period,
        DTTMHIDE as dttm_hide,
        
        -- Migration tracking
        KEYMIGRATIONSOURCE as key_migration_source,
        TYPMIGRATIONSOURCE as type_migration_source,
        
        -- User-defined fields
        USERTXT1 as user_txt_1,
        USERTXT2 as user_txt_2,
        USERTXT3 as user_txt_3,
        USERNUM1 as user_num_1,
        USERNUM2 as user_num_2,
        USERNUM3 as user_num_3,
        
        -- System fields
        SYSSEQ as sys_seq,
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