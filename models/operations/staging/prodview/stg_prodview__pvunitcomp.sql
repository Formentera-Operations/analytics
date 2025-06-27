with source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMP') }}
),

renamed as (
    select
        -- Primary identifiers
        IDREC as id_rec,
        IDRECPARENT as id_rec_parent,
        IDFLOWNET as id_flow_net,
        
        -- Completion identifiers
        COMPIDA as comp_id_a,
        COMPIDB as comp_id_b,
        COMPIDC as comp_id_c,
        COMPIDD as comp_id_d,
        COMPLETIONIDE as comp_id_e,
        COMPIDPA as comp_id_pa,
        COMPLETIONCODE as completion_code,
        COMPIDREGULATORY as comp_id_regulatory,
        COMPLETIONNAME as completion_name,
        COMPLETIONLICENSEE as completion_licensee,
        COMPLETIONLICENSENO as completion_license_no,
        
        -- Well identifiers
        WELLIDA as well_id_a,
        WELLIDB as well_id_b,
        WELLIDC as well_id_c,
        WELLIDD as well_id_d,
        WELLIDE as well_id_e,
        WELLIDPA as well_id_pa,
        WELLIDREGULATORY as well_id_regulatory,
        WELLLICENSENO as well_license_no,
        WELLNAME as well_name,
        
        -- Operational dates
        DTTMSTARTALLOC as alloc_start_date,
        DTTMEND as comp_dttm_end,
        DTTMLASTPRODUCEDCALC as dttm_last_produced_calc,
        DTTMLASTPRODUCEDHCLIQCALC as dttm_last_produced_hc_liq_calc,
        DTTMLASTPRODUCEDGASCALC as dttm_last_produced_gas_calc,
        DTTMFIRSTSALE as first_sale_date,
        DTTMFLOWBACKEND as flowback_end_date,
        DTTMFLOWBACKSTART as flowback_start_date,
        DTTMABANDON as abandon_date,
        DTTMONPROD as production_date,
        DTTMLICENSE as dttm_license,
        
        -- Geographic coordinates
        LATITUDE as comp_latitude,
        LONGITUDE as comp_longitude,
        LATLONGSOURCE as comp_lat_long_source,
        LATLONGDATUM as comp_lat_long_datum,
        
        -- Production thresholds and parameters
        HELDBYPRODUCTIONTHRESHOLD as held_by_production_threshold,
        ENTRYREQPERIODFLUIDLEVEL as entry_req_period_fluid_level,
        ENTRYREQPERIODPARAM as entry_req_period_param,
        
        -- Export/Import tracking
        EXPORTID1 as export_id_1,
        EXPORTID2 as export_id_2,
        EXPORTTYP1 as export_type_1,
        EXPORTTYP2 as export_type_2,
        IMPORTID1 as import_id_1,
        IMPORTID2 as import_id_2,
        IMPORTTYP1 as import_type_1,
        IMPORTTYP2 as import_type_2,
        
        -- Migration tracking
        KEYMIGRATIONSOURCE as comp_key_migration_source,
        TYPMIGRATIONSOURCE as comp_type_migration_source,
        
        -- User-defined text fields
        USERTXT1 as comp_user_txt_1,
        USERTXT2 as comp_user_txt_2,
        USERTXT3 as comp_user_txt_3,
        USERTXT4 as comp_user_txt_4,
        USERTXT5 as comp_user_txt_5,
        
        -- User-defined numeric fields
        USERNUM1 as comp_user_num_1,
        USERNUM2 as comp_user_num_2,
        USERNUM3 as comp_user_num_3,
        USERNUM4 as comp_user_num_4,
        USERNUM5 as comp_user_num_5,
        
        -- User-defined datetime fields
        USERDTTM1 as comp_user_dttm_1,
        USERDTTM2 as comp_user_dttm_2,
        USERDTTM3 as comp_user_dttm_3,
        USERDTTM4 as comp_user_dttm_4,
        USERDTTM5 as comp_user_dttm_5,
        
        -- System audit fields
        SYSCREATEDATE as comp_sys_create_date,
        SYSCREATEUSER as comp_sys_create_user,
        SYSMODDATE as comp_sys_mod_date,
        SYSMODUSER as comp_sys_mod_user,
        
        -- Fivetran metadata
        _FIVETRAN_SYNCED as update_date,
        _FIVETRAN_DELETED as deleted

    from source
)

select * from renamed