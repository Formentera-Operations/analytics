{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVFACRECDISPCALC') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idflownet)::varchar as id_flownet,

        -- dates
        dttm::timestamp_ntz as transaction_date,
        year::float as transaction_year,
        month::float as transaction_month,
        dayofmonth::float as day_of_month,

        -- sending entity references
        trim(idrecunitsend)::varchar as sending_unit_id,
        trim(idrecunitnodesend)::varchar as sending_unit_node_id,
        trim(idrecfacilitysend)::varchar as sending_facility_id,
        trim(idflownetsend)::varchar as sending_flow_network_id,

        -- receiving entity references
        trim(idrecunitrec)::varchar as receiving_unit_id,
        trim(idrecunitnoderec)::varchar as receiving_unit_node_id,
        trim(idrecfacilityrec)::varchar as receiving_facility_id,
        trim(idflownetrec)::varchar as receiving_flow_network_id,

        -- volume measurements (converted to US units)
        {{ pv_cbm_to_bbl('volhcliq') }}::float as hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volgas') }}::float as gas_volume_mcf,
        {{ pv_cbm_to_mcf('volgasplusgaseq') }}::float as gas_plus_gas_equivalent_mcf,
        {{ pv_cbm_to_bbl('volwater') }}::float as water_volume_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as sand_volume_bbl,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as facility_daily_receipt_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        facility_daily_receipt_sk,

        -- identifiers
        id_rec,
        id_flownet,

        -- dates
        transaction_date,
        transaction_year,
        transaction_month,
        day_of_month,

        -- sending entity references
        sending_unit_id,
        sending_unit_node_id,
        sending_facility_id,
        sending_flow_network_id,

        -- receiving entity references
        receiving_unit_id,
        receiving_unit_node_id,
        receiving_facility_id,
        receiving_flow_network_id,

        -- volume measurements
        hcliq_volume_bbl,
        gas_volume_mcf,
        gas_plus_gas_equivalent_mcf,
        water_volume_bbl,
        sand_volume_bbl,

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
