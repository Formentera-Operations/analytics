{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODEFLOWTO') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as id_rec_parent,
        trim(idflownet)::varchar as id_flownet,

        -- connection period
        dttmstart::timestamp_ntz as connection_start_date,
        dttmend::timestamp_ntz as connection_end_date,

        -- inlet information
        trim(idrecinlet)::varchar as inlet_id,
        trim(idrecinlettk)::varchar as inlet_table,
        trim(idrecinletunitcalc)::varchar as inlet_unit_id_calculated,
        trim(idrecinletunitcalctk)::varchar as inlet_unit_table_calculated,

        -- outlet information (calculated)
        trim(idrecoutletcalc)::varchar as outlet_id_calculated,
        trim(idrecoutletcalctk)::varchar as outlet_table_calculated,
        trim(idrecoutletunitcalc)::varchar as outlet_unit_id_calculated,
        trim(idrecoutletunitcalctk)::varchar as outlet_unit_table_calculated,

        -- flow characteristics
        recircflow::boolean as is_recirculation_flow,
        trim(com)::varchar as comments,

        -- sequence
        sysseq::float as sequence_number,

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
        and id_rec is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as unit_node_connection_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        unit_node_connection_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- connection period
        connection_start_date,
        connection_end_date,

        -- inlet information
        inlet_id,
        inlet_table,
        inlet_unit_id_calculated,
        inlet_unit_table_calculated,

        -- outlet information (calculated)
        outlet_id_calculated,
        outlet_table_calculated,
        outlet_unit_id_calculated,
        outlet_unit_table_calculated,

        -- flow characteristics
        is_recirculation_flow,
        comments,

        -- sequence
        sequence_number,

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

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
