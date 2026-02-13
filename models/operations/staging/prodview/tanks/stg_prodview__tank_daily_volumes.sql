{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKMONTHDAYCALC') }}
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

        -- tank reference
        trim(idrectank)::varchar as tank_id,
        trim(idrectanktk)::varchar as tank_table_key,

        -- dates
        dttm::timestamp_ntz as tank_date,
        year::int as tank_year,
        month::int as tank_month,
        dayofmonth::int as day_of_month,

        -- opening inventory volumes (converted to imperial units)
        {{ pv_cbm_to_bbl('volstarttotal') }}::float as opening_total_volume_bbl,
        {{ pv_cbm_to_bbl('volstarthcliq') }}::float as opening_oil_condensate_volume_bbl,
        {{ pv_cbm_to_mcf('volstarthcliqgaseq') }}::float as opening_gas_equivalent_oil_cond_volume_mcf,
        {{ pv_cbm_to_bbl('volstartwater') }}::float as opening_water_volume_bbl,
        {{ pv_cbm_to_bbl('volstartsand') }}::float as opening_sand_volume_bbl,

        -- opening inventory percentages
        {{ pv_decimal_to_pct('bswstart') }}::float as opening_bsw_total_pct,
        {{ pv_decimal_to_pct('sandcutstart') }}::float as opening_sand_cut_total_pct,

        -- closing inventory volumes (converted to imperial units)
        {{ pv_cbm_to_bbl('volendtotal') }}::float as closing_total_volume_bbl,
        {{ pv_cbm_to_bbl('volendhcliq') }}::float as closing_oil_condensate_volume_bbl,
        {{ pv_cbm_to_mcf('volendhcliqgaseq') }}::float as closing_gas_equivalent_oil_cond_volume_mcf,
        {{ pv_cbm_to_bbl('volendwater') }}::float as closing_water_volume_bbl,
        {{ pv_cbm_to_bbl('volendsand') }}::float as closing_sand_volume_bbl,

        -- closing inventory percentages
        {{ pv_decimal_to_pct('bswend') }}::float as closing_bsw_total_pct,
        {{ pv_decimal_to_pct('sandcutend') }}::float as closing_sand_cut_total_pct,

        -- change in inventory volumes (converted to imperial units)
        {{ pv_cbm_to_bbl('volchgtotal') }}::float as change_total_volume_bbl,
        {{ pv_cbm_to_bbl('volchghcliq') }}::float as change_oil_condensate_volume_bbl,
        {{ pv_cbm_to_mcf('volchghcliqgaseq') }}::float as change_gas_equivalent_oil_cond_volume_mcf,
        {{ pv_cbm_to_bbl('volchgwater') }}::float as change_water_volume_bbl,
        {{ pv_cbm_to_bbl('volchgsand') }}::float as change_sand_volume_bbl,

        -- facility references
        trim(idrecfacility)::varchar as current_facility_id,
        trim(idrecfacilitytk)::varchar as current_facility_table,
        trim(idrechcliqanalysis)::varchar as hc_liquid_analysis_id,
        trim(idrechcliqanalysistk)::varchar as hc_liquid_analysis_table,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as tank_daily_volume_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tank_daily_volume_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- tank reference
        tank_id,
        tank_table_key,

        -- dates
        tank_date,
        tank_year,
        tank_month,
        day_of_month,

        -- opening inventory volumes
        opening_total_volume_bbl,
        opening_oil_condensate_volume_bbl,
        opening_gas_equivalent_oil_cond_volume_mcf,
        opening_water_volume_bbl,
        opening_sand_volume_bbl,
        opening_bsw_total_pct,
        opening_sand_cut_total_pct,

        -- closing inventory volumes
        closing_total_volume_bbl,
        closing_oil_condensate_volume_bbl,
        closing_gas_equivalent_oil_cond_volume_mcf,
        closing_water_volume_bbl,
        closing_sand_volume_bbl,
        closing_bsw_total_pct,
        closing_sand_cut_total_pct,

        -- change in inventory volumes
        change_total_volume_bbl,
        change_oil_condensate_volume_bbl,
        change_gas_equivalent_oil_cond_volume_mcf,
        change_water_volume_bbl,
        change_sand_volume_bbl,

        -- facility references
        current_facility_id,
        current_facility_table,
        hc_liquid_analysis_id,
        hc_liquid_analysis_table,

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
