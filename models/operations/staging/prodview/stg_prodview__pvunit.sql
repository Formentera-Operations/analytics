
with source as (
    select * from {{ source('prodview', 'PVT_PVUNIT') }}
),

renamed as (
    select
        -- Primary identifiers
        idflownet as id_flow_net,
        idrec as id_rec,
        name,
        nameshort as name_short,
        
        -- Unit types and classifications
        typ1 as type_1,
        typ2 as type_2,
        typdisphcliq as type_disp_hc_liq,
        typdispngl as type_disp_ngl,
        dispproductname as disp_product_name,
        typregulatory as type_regulatory,
        typpa as type_pa,
        
        -- Calculated references
        idrecroutesetroutecalc as id_rec_routeset_route_calc,
        idrecroutesetroutecalctk as id_rec_routeset_route_calc_tk,
        idrecfacilitycalc as id_rec_facility_calc,
        idrecfacilitycalctk as id_rec_facility_calc_tk,
        idreccompstatuscalc as id_rec_comp_status_calc,
        idreccompstatuscalctk as id_rec_comp_status_calc_tk,
        
        -- Display and UI settings
        displaysizefactor as display_size_factor,
        
        -- Operational dates
        dttmstart as dttm_start,
        dttmend as dttm_end,
        dttmhide as dttm_hide,
        
        -- Elevation with unit conversion (meters to feet)
        elevation / 0.3048 as elevation,
        case 
            when elevation is not null then 'FT'
            else null 
        end as elevation_unit_label,
        
        -- Regulatory and PA identifiers
        unitidregulatory as unit_id_regulatory,
        unitidpa as unit_id_pa,
        stopname as stop_name,
        unitida as unit_id_a,
        unitidb as unit_id_b,
        unitidc as unit_id_c,
        
        -- Operational information
        purchaser,
        operated,
        operator,
        operatorida as operator_id_a,
        com,
        legalsurfloc as legal_surf_loc,
        
        -- Geographic hierarchy
        division,
        divisioncode as division_code,
        district,
        country,
        area,
        field,
        fieldcode as field_code,
        fieldoffice as field_office,
        fieldofficecode as field_office_code,
        stateprov as state_prov,
        county,
        
        -- Geographic coordinates
        latitude,
        longitude,
        latlongsource as lat_long_source,
        latlongdatum as lat_long_datum,
        
        -- UTM coordinates with unit labels
        utmgridzone as utm_grid_zone,
        utmsource as utm_source,
        utmx as utm_x,
        case 
            when utmx is not null then 'M'
            else null 
        end as utm_x_unit_label,
        utmy as utm_y,
        case 
            when utmy is not null then 'M'
            else null 
        end as utm_y_unit_label,
        
        -- Location details
        lease,
        leaseida as lease_id_a,
        locationtyp as location_type,
        platform,
        padcode as pad_code,
        padname as pad_name,
        slot,
        govauthority as gov_authority,
        
        -- Business information
        costcenterida as cost_center_id_a,
        costcenteridb as cost_center_id_b,
        sortbyuser as sort_by_user,
        priority,
        timezone,
        
        -- Responsibility assignments
        idrecresp1 as id_rec_resp_1,
        idrecresp1tk as id_rec_resp_1_tk,
        idrecresp2 as id_rec_resp_2,
        idrecresp2tk as id_rec_resp_2_tk,
        
        -- Migration tracking
        keymigrationsource as key_migration_source,
        typmigrationsource as type_migration_source,
        
        -- User-defined text fields
        usertxt1 as user_txt_1,
        usertxt2 as user_txt_2,
        usertxt3 as user_txt_3,
        usertxt4 as user_txt_4,
        usertxt5 as user_txt_5,
        
        -- User-defined numeric fields
        usernum1 as user_num_1,
        usernum2 as user_num_2,
        usernum3 as user_num_3,
        usernum4 as user_num_4,
        usernum5 as user_num_5,
        
        -- User-defined datetime fields
        userdttm1 as user_dttm_1,
        userdttm2 as user_dttm_2,
        userdttm3 as user_dttm_3,
        userdttm4 as user_dttm_4,
        userdttm5 as user_dttm_5,
        
        -- System locking fields
        syslockmeui as sys_lock_me_ui,
        syslockchildrenui as sys_lock_children_ui,
        syslockme as sys_lock_me,
        syslockchildren as sys_lock_children,
        syslockdate as sys_lock_date,
        
        -- System audit fields
        sysmoddate as sys_mod_date,
        sysmoduser as sys_mod_user,
        syscreatedate as sys_create_date,
        syscreateuser as sys_create_user,
        systag as sys_tag,
        
        -- Fivetran metadata mapped to business fields
        _fivetran_synced as update_date,
        _fivetran_deleted as deleted

    from source
)

select * from renamed