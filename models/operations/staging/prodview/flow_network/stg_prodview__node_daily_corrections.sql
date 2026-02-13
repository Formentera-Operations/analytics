{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODECORRDAY') }}
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

        -- dates
        dttm::timestamp_ntz as correction_date,

        -- volume corrections (converted to US units)
        {{ pv_cbm_to_bbl('volhcliq') }}::float as final_hcliq_bbl,
        {{ pv_cbm_to_mcf('volgas') }}::float as final_gas_mcf,
        {{ pv_cbm_to_bbl('volwater') }}::float as final_water_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as final_sand_bbl,

        -- heat content (converted to US units)
        {{ pv_joules_to_mmbtu('heat') }}::float as final_heat_mmbtu,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as node_daily_correction_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        node_daily_correction_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        correction_date,

        -- volume corrections
        final_hcliq_bbl,
        final_gas_mcf,
        final_water_bbl,
        final_sand_bbl,

        -- heat content
        final_heat_mmbtu,

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
