{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKSTARTINV') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idrecparent)::varchar as tank_id,
        trim(idflownet)::varchar as flow_network_id,

        -- origin references
        trim(idrecunitorigin)::varchar as originating_unit_id,
        trim(idrecunitorigintk)::varchar as originating_unit_table,
        trim(idreccomporigin)::varchar as originating_completion_id,
        trim(idreccomporigintk)::varchar as originating_completion_table,

        -- dates
        dttminv::timestamp_ntz as inventory_date,

        -- volumes
        {{ pv_cbm_to_bbl('volhcliq') }} as hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volgas') }} as gas_volume_mcf,
        {{ pv_cbm_to_bbl('volwater') }} as water_volume_bbl,
        {{ pv_cbm_to_bbl('volsand') }} as sand_volume_bbl,

        -- flags
        keepwhole::boolean as is_keep_whole,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
        trim(systag)::varchar as record_tag,
        syslockdate::timestamp_ntz as lock_date_utc,
        syslockme::boolean as is_locked,
        syslockchildren::boolean as is_children_locked,
        syslockmeui::boolean as is_locked_ui,
        syslockchildrenui::boolean as is_children_locked_ui,

        -- ingestion metadata
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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as tank_starting_inventory_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tank_starting_inventory_sk,

        -- identifiers
        id_rec,
        tank_id,
        flow_network_id,

        -- origin references
        originating_unit_id,
        originating_unit_table,
        originating_completion_id,
        originating_completion_table,

        -- dates
        inventory_date,

        -- volumes
        hcliq_volume_bbl,
        gas_volume_mcf,
        water_volume_bbl,
        sand_volume_bbl,

        -- flags
        is_keep_whole,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        record_tag,
        lock_date_utc,
        is_locked,
        is_children_locked,
        is_locked_ui,
        is_children_locked_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
