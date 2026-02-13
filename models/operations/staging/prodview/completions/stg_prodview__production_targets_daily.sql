{{
    config(
        materialized='view',
        tags=['prodview', 'completions', 'targets', 'daily', 'staging']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTARGETDAY') }}
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
        dttm::date as target_daily_date,

        -- target rates - liquids (converted from m3/day to bbl/day)
        {{ pv_cbm_to_bbl_per_day('ratehcliq') }}::float as target_daily_rate_hcliq_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('rateoil') }}::float as target_daily_rate_oil_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratecond') }}::float as target_daily_rate_condensate_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratengl') }}::float as target_daily_rate_ngl_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratewater') }}::float as target_daily_rate_water_bbl_per_day,
        {{ pv_cbm_to_bbl_per_day('ratesand') }}::float as target_daily_rate_sand_bbl_per_day,

        -- target rates - gas (converted from m3/day to mcf/day)
        {{ pv_cbm_to_mcf('rategas') }}::float as target_daily_rate_gas_mcf_per_day,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as production_target_daily_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        production_target_daily_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        target_daily_date,

        -- target rates - liquids
        target_daily_rate_hcliq_bbl_per_day,
        target_daily_rate_oil_bbl_per_day,
        target_daily_rate_condensate_bbl_per_day,
        target_daily_rate_ngl_bbl_per_day,
        target_daily_rate_water_bbl_per_day,
        target_daily_rate_sand_bbl_per_day,

        -- target rates - gas
        target_daily_rate_gas_mcf_per_day,

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
