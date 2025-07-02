{{ config(
    materialized='view',
    tags=['prodview', 'units', 'facilities', 'staging']
) }}

with source_data as (
    select * from {{ source('prodview', 'PVT_PVUNIT') }}
    where _fivetran_deleted = false
),

renamed as (
    select
        -- Primary identifiers
        idrec as unit_id,
        idflownet as flow_network_id,
        name as unit_name,
        nameshort as display_name,
        
        -- Unit classification
        typ1 as unit_type,
        typ2 as unit_sub_type,
        typregulatory as regulatory_unit_type,
        typdisphcliq as hcliq_inventory_type,
        typdispngl as ngl_inventory_type,
        dispproductname as hcliq_disposition_method,
        typpa as comingle_permit_no,
        
        -- Status and operational info
        priority as is_cycled,
        operated as is_operated,
        operator as operator_name,
        operatorida as operated_descriptor,
        purchaser as is_purchaser,
        displaysizefactor as display_size_factor,
        
        -- Temporal information
        dttmstart as start_displaying_date,
        dttmend as stop_displaying_date,
        dttmhide as hide_record_date,
        
        -- Location - Geographic coordinates
        latitude as surface_latitude,
        longitude as surface_longitude,
        latlongsource as lat_long_source,
        latlongdatum as lat_long_datum,
        
        -- Location - UTM coordinates (keeping in meters)
        utmgridzone as utm_grid_zone,
        utmx as utm_easting_m,
        utmy as utm_northing_m,
        utmsource as utm_source,
        
        -- Location - Physical/Administrative
        elevation / 0.3048 as ground_elevation_ft,
        legalsurfloc as surface_legal_location,
        division as division,
        divisioncode as company_code,
        district as district,
        area as asset_co,
        field as foreman_area,
        fieldcode as regulatory_field_name,
        fieldoffice as field_office,
        fieldofficecode as district_office,
        country as country,
        stateprov as state_province,
        county as county,
        
        -- Facility and infrastructure
        platform as route,
        padcode as facility_name,
        padname as pad_name,
        slot as dsu,
        locationtyp as swd_system,
        
        -- Business identifiers
        unitidregulatory as regulatory_id,
        unitidpa as eid,
        unitida as api_10,
        unitidb as property_number,
        unitidc as combo_curve_id,
        stopname as stop_name,
        lease as lease_name,
        leaseida as lease_number_tx_luw_la,
        
        -- Financial and organizational
        costcenterida as cost_center,
        costcenteridb as gas_gathering_system_name,
        govauthority as government_authority,
        
        -- Current status references (calculated fields)
        idrecroutesetroutecalc as current_route_id,
        idrecroutesetroutecalctk as current_route_table,
        idrecfacilitycalc as current_facility_id,
        idrecfacilitycalctk as current_facility_table,
        idreccompstatuscalc as current_completion_status_id,
        idreccompstatuscalctk as current_completion_status_table,
        
        -- Responsible parties
        idrecresp1 as oil_purchaser_id,
        idrecresp1tk as primary_responsible_table,
        idrecresp2 as gas_purchaser_id,
        idrecresp2tk as secondary_responsible_table,
        
        -- Migration and integration
        keymigrationsource as migration_source_key,
        typmigrationsource as migration_source_type,
        
        -- User-defined fields - Text
        usertxt1 as user_text_1,
        usertxt2 as completion_status,
        usertxt3 as producing_method,
        usertxt4 as stripper_type,
        usertxt5 as chemical_provider,
        
        -- User-defined fields - Numeric
        usernum1 as electric_allocation_meter_number,
        usernum2 as electric_meter_id,
        usernum3 as electric_acct_no,
        usernum4 as electric_vendor_no,
        usernum5 as user_num_5,
        
        -- User-defined fields - Datetime
        userdttm1 as stripper_date,
        userdttm2 as bha_change_1,
        userdttm3 as bha_change_2,
        userdttm4 as user_date_4,
        userdttm5 as user_date_5,
        
        -- Administrative
        sortbyuser as unit_sort,
        timezone as timezone,
        com as comments,
        
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