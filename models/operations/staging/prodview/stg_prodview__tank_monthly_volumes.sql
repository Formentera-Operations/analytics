{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITTANKMONTHCALC') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as tank_calculation_id,
        trim(idrecparent)::varchar as parent_tank_id,
        trim(idflownet)::varchar as flow_network_id,

        -- dates
        dttmstart::timestamp_ntz as period_start_date,
        dttmend::timestamp_ntz as period_end_date,
        year::int as calculation_year,
        month::int as calculation_month,

        -- opening inventory volumes
        {{ pv_cbm_to_bbl('volstarttotal') }} as opening_total_volume_bbl,
        {{ pv_cbm_to_bbl('volstarthcliq') }} as opening_hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volstarthcliqgaseq') }} as opening_gas_equivalent_hcliq_volume_mcf,
        {{ pv_cbm_to_bbl('volstartwater') }} as opening_water_volume_bbl,
        {{ pv_cbm_to_bbl('volstartsand') }} as opening_sand_volume_bbl,

        -- opening quality measurements
        {{ pv_decimal_to_pct('bswstart') }} as opening_bsw_total_pct,
        {{ pv_decimal_to_pct('sandcutstart') }} as opening_sand_cut_total_pct,

        -- closing inventory volumes
        {{ pv_cbm_to_bbl('volendtotal') }} as closing_total_volume_bbl,
        {{ pv_cbm_to_bbl('volendhcliq') }} as closing_hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volendhcliqgaseq') }} as closing_gas_equiv_hcliq_volume_mcf,
        {{ pv_cbm_to_bbl('volendwater') }} as closing_water_volume_bbl,
        {{ pv_cbm_to_bbl('volendsand') }} as closing_sand_volume_bbl,

        -- closing quality measurements
        {{ pv_decimal_to_pct('bswend') }} as closing_bsw_total_pct,
        {{ pv_decimal_to_pct('sandcutend') }} as closing_sand_cut_total_pct,

        -- change in inventory
        {{ pv_cbm_to_bbl('volchgtotal') }} as change_in_total_volume_bbl,
        {{ pv_cbm_to_bbl('volchghcliq') }} as change_in_hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volchghcliqgaseq') }} as change_in_gas_equivalent_hcliq_volume_mcf,
        {{ pv_cbm_to_bbl('volchgwater') }} as change_in_water_volume_bbl,
        {{ pv_cbm_to_bbl('volchgsand') }} as change_in_sand_volume_bbl,

        -- facility and analysis references
        trim(idrecfacility)::varchar as current_facility_id,
        trim(idrecfacilitytk)::varchar as current_facility_table,
        trim(idrechcliqanalysis)::varchar as hc_liquid_analysis_id,
        trim(idrechcliqanalysistk)::varchar as hc_liquid_analysis_table,

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
        and tank_calculation_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['tank_calculation_id']) }} as tank_monthly_volume_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        tank_monthly_volume_sk,

        -- identifiers
        tank_calculation_id,
        parent_tank_id,
        flow_network_id,

        -- dates
        period_start_date,
        period_end_date,
        calculation_year,
        calculation_month,

        -- opening inventory volumes
        opening_total_volume_bbl,
        opening_hcliq_volume_bbl,
        opening_gas_equivalent_hcliq_volume_mcf,
        opening_water_volume_bbl,
        opening_sand_volume_bbl,

        -- opening quality measurements
        opening_bsw_total_pct,
        opening_sand_cut_total_pct,

        -- closing inventory volumes
        closing_total_volume_bbl,
        closing_hcliq_volume_bbl,
        closing_gas_equiv_hcliq_volume_mcf,
        closing_water_volume_bbl,
        closing_sand_volume_bbl,

        -- closing quality measurements
        closing_bsw_total_pct,
        closing_sand_cut_total_pct,

        -- change in inventory
        change_in_total_volume_bbl,
        change_in_hcliq_volume_bbl,
        change_in_gas_equivalent_hcliq_volume_mcf,
        change_in_water_volume_bbl,
        change_in_sand_volume_bbl,

        -- facility and analysis references
        current_facility_id,
        current_facility_table,
        hc_liquid_analysis_id,
        hc_liquid_analysis_table,

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
