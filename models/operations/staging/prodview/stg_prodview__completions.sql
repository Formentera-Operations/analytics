{{ config(
    materialized='view',
    tags=['prodview', 'completions', 'wells', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMP') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as completion_id,
        idrecparent as unit_id,
        idflownet as flow_network_id,
        
        -- Completion lifecycle dates
        dttmstartalloc as start_allocating_date_in_prodview,
        dttmend as expiry_date,
        dttmonprod as pop_date,
        dttmfirstsale as first_sale_date,
        dttmflowbackstart as flowback_start_date,
        dttmflowbackend as flowback_end_date,
        dttmabandon as abandon_date,
        dttmlastproducedcalc as last_produced_date,
        dttmlastproducedhcliqcalc as last_produced_oil_date,
        dttmlastproducedgascalc as last_produced_gas_date,
        
        -- Production threshold
        heldbyproductionthreshold as held_by_production_threshold_days,
        
        -- Completion identifiers
        completionname as completion_name,
        permanentid as permanent_completion_id,
        compidregulatory as ghg_report_basin,
        compidpa as production_accounting_identifier_completion,
        completionlicensee as completion_licensee,
        completionlicenseno as federal_lease_number,
        dttmlicense as completion_license_date,
        compida as well_number,
        compidb as gas_pop_id,
        compidc as gas_meter_no,
        compidd as gas_alloc_meter_no,
        completionide as gas_alloc_group_no,
        completioncode as surface_commingle_number,
        
        -- Well identifiers
        wellname as well_name,
        wellidregulatory as regulatory_id_of_well,
        wellidpa as production_accounting_identifier_well,
        welllicenseno as well_license_number_la_serial_nd_file_tx_ms_ok_api14,
        wellida as api10,
        wellidb as cost_center,
        wellidc as eid,
        wellidd as producing_formation,
        wellide as legal_well_name,
        
        -- Import/Export tracking
        importid1 as import_id_1,
        importtyp1 as import_type_1,
        importid2 as import_id_2,
        importtyp2 as import_type_2,
        exportid1 as export_id_1,
        exporttyp1 as export_type_1,
        exportid2 as export_id_2,
        exporttyp2 as export_type_2,
        
        -- Location information
        latitude as bottomhole_latitude,
        longitude as bottomhole_longitude,
        latlongsource as lat_long_data_source,
        latlongdatum as lat_long_datum,
        
        -- Entry requirements
        entryreqperiodfluidlevel as entry_requirement_period_fluid_level,
        entryreqperiodparam as entry_requirement_period_parameters,
        
        -- User-defined fields - Text
        usertxt1 as bha_type_paga_saga_rpga,
        usertxt2 as rescat,
        usertxt3 as electric_vendor_name,
        usertxt4 as electric_meter_name,
        usertxt5 as working_interest_partner,
        
        -- User-defined fields - Numeric
        usernum1 as surface_casing,
        usernum2 as prod_casing,
        usernum3 as prod_liner,
        usernum4 as purchaser_ctb_lease_id,
        usernum5 as purchaser_well_lease_id,
        
        -- User-defined fields - Datetime
        userdttm1 as spud_date,
        userdttm2 as spud_date_2,
        userdttm3 as rig_release_date,
        userdttm4 as user_date_4,
        userdttm5 as user_date_5,
        
        -- Migration and integration
        keymigrationsource as migration_source_key,
        typmigrationsource as migration_source_type,
        
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