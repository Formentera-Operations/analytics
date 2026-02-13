{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODEMONTHDAYCALC') }}
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
        trim(idrecnode)::varchar as node_id,
        trim(idrecnodetk)::varchar as node_table,

        -- dates
        dttm::timestamp_ntz as calculation_date,
        year::float as calculation_year,
        month::float as calculation_month,
        dayofmonth::float as day_of_month,

        -- volumes (converted to US units)
        {{ pv_cbm_to_bbl('volhcliq') }}::float as hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volhcliqgaseq') }}::float as hcliq_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volgas') }}::float as gas_volume_mcf,
        {{ pv_cbm_to_bbl('volwater') }}::float as water_volume_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as sand_volume_bbl,

        -- heat content (converted to US units)
        {{ pv_joules_to_mmbtu('heat') }}::float as heat_content_mmbtu,
        {{ pv_jm3_to_btu_per_ft3('factheat') }}::float as heat_factor_btu_per_ft3,

        -- facility reference
        trim(idrecfacility)::varchar as facility_id,
        trim(idrecfacilitytk)::varchar as facility_table,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as node_daily_volume_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        node_daily_volume_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        node_id,
        node_table,

        -- dates
        calculation_date,
        calculation_year,
        calculation_month,
        day_of_month,

        -- volumes
        hcliq_volume_bbl,
        hcliq_gas_equivalent_mcf,
        gas_volume_mcf,
        water_volume_bbl,
        sand_volume_bbl,

        -- heat content
        heat_content_mmbtu,
        heat_factor_btu_per_ft3,

        -- facility reference
        facility_id,
        facility_table,

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
