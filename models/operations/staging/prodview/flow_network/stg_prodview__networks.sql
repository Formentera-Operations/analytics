{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVFLOWNETHEADER') }}
    qualify 1 = row_number() over (
        partition by idflownet
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idflownet)::varchar as id_flownet,
        trim(name)::varchar as flow_network_name,
        trim(typ)::varchar as flow_network_type,

        -- primary unit and facility references
        trim(idrecunitprimary)::varchar as primary_unit_id,
        trim(idrecunitprimarytk)::varchar as primary_unit_table,
        trim(idrecfacilityprimary)::varchar as primary_facility_id,
        trim(idrecfacilityprimarytk)::varchar as primary_facility_table,

        -- general information
        trim(com)::varchar as comments,

        -- responsibility assignments
        trim(idrecresp1)::varchar as primary_responsible_id,
        trim(idrecresp1tk)::varchar as primary_responsible_table,
        trim(idrecresp2)::varchar as secondary_responsible_id,
        trim(idrecresp2tk)::varchar as secondary_responsible_table,

        -- reporting configuration flags
        rptgatheredcalcs::boolean as report_gathered_calculations,
        rptallocations::boolean as report_allocations,
        rptdispositions::boolean as report_dispositions,
        rptcomponentdispositions::boolean as report_component_dispositions,
        rptnodecalculations::boolean as report_node_calculations,

        -- operational settings
        trackdownholeinventory::boolean as track_downhole_inventory,

        -- allocation and process dates
        dttmallocprocessbegan::timestamp_ntz as allocation_process_began_date,
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,
        dttmlastallocprocess::timestamp_ntz as last_allocation_process_date,
        trim(userlastallocprocess)::varchar as last_allocation_process_user,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        trim(usertxt4)::varchar as user_txt4,
        trim(usertxt5)::varchar as user_txt5,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,
        trim(systag)::varchar as record_tag,

        -- additional system fields
        sysmoddatedb::timestamp_ntz as database_modified_at_utc,
        trim(sysmoduserdb)::varchar as database_modified_by,
        trim(syssecuritytyp)::varchar as security_type,
        syslockdatemaster::timestamp_ntz as master_lock_date_utc,

        -- fivetran metadata
        _fivetran_deleted::boolean as _fivetran_deleted,
        _fivetran_synced::timestamp_tz as _fivetran_synced

    from source
),

filtered as (
    select *
    from renamed
    where
        coalesce(_fivetran_deleted, false) = false
        and id_flownet is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_flownet']) }} as network_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        network_sk,

        -- identifiers
        id_flownet,
        flow_network_name,
        flow_network_type,

        -- primary unit and facility references
        primary_unit_id,
        primary_unit_table,
        primary_facility_id,
        primary_facility_table,

        -- general information
        comments,

        -- responsibility assignments
        primary_responsible_id,
        primary_responsible_table,
        secondary_responsible_id,
        secondary_responsible_table,

        -- reporting configuration flags
        report_gathered_calculations,
        report_allocations,
        report_dispositions,
        report_component_dispositions,
        report_node_calculations,

        -- operational settings
        track_downhole_inventory,

        -- allocation and process dates
        allocation_process_began_date,
        start_date,
        end_date,
        last_allocation_process_date,
        last_allocation_process_user,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_txt4,
        user_txt5,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,
        record_tag,
        database_modified_at_utc,
        database_modified_by,
        security_type,
        master_lock_date_utc,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
