{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITDISPMONTHDAY') }}
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
        dttm::timestamp_ntz as disposition_date,
        year::int as disposition_year,
        month::int as disposition_month,
        dayofmonth::int as day_of_month,

        -- unit and completion references
        trim(idrecunit)::varchar as unit_id,
        trim(idrecunittk)::varchar as unit_table,
        trim(idreccomp)::varchar as completion_id,
        trim(idreccomptk)::varchar as completion_table,
        trim(idreccompzone)::varchar as reporting_contact_interval_id,
        trim(idreccompzonetk)::varchar as reporting_contact_interval_table,

        -- outlet and disposition references
        trim(idrecoutletsend)::varchar as outlet_send_id,
        trim(idrecoutletsendtk)::varchar as outlet_send_table,
        trim(idrecdispunitnode)::varchar as disposition_unit_node_id,
        trim(idrecdispunitnodetk)::varchar as disposition_unit_node_table,
        trim(idrecdispunit)::varchar as disposition_unit_id,
        trim(idrecdispunittk)::varchar as disposition_unit_table,

        -- total fluid volumes (converted to US units)
        {{ pv_cbm_to_bbl('volhcliq') }}::float as hcliq_volume_bbl,
        {{ pv_cbm_to_mcf('volhcliqgaseq') }}::float as hcliq_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volgas') }}::float as gas_volume_mcf,
        {{ pv_cbm_to_bbl('volwater') }}::float as water_volume_bbl,
        {{ pv_cbm_to_bbl('volsand') }}::float as sand_volume_bbl,

        -- C1 (Methane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volc1liq') }}::float as c1_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volc1gaseq') }}::float as c1_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volc1gas') }}::float as c1_gas_volume_mcf,

        -- C2 (Ethane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volc2liq') }}::float as c2_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volc2gaseq') }}::float as c2_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volc2gas') }}::float as c2_gas_volume_mcf,

        -- C3 (Propane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volc3liq') }}::float as c3_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volc3gaseq') }}::float as c3_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volc3gas') }}::float as c3_gas_volume_mcf,

        -- iC4 (Iso-butane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volic4liq') }}::float as ic4_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volic4gaseq') }}::float as ic4_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volic4gas') }}::float as ic4_gas_volume_mcf,

        -- nC4 (Normal butane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volnc4liq') }}::float as nc4_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volnc4gaseq') }}::float as nc4_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volnc4gas') }}::float as nc4_gas_volume_mcf,

        -- iC5 (Iso-pentane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volic5liq') }}::float as ic5_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volic5gaseq') }}::float as ic5_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volic5gas') }}::float as ic5_gas_volume_mcf,

        -- nC5 (Normal pentane) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volnc5liq') }}::float as nc5_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volnc5gaseq') }}::float as nc5_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volnc5gas') }}::float as nc5_gas_volume_mcf,

        -- C6 (Hexanes) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volc6liq') }}::float as c6_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volc6gaseq') }}::float as c6_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volc6gas') }}::float as c6_gas_volume_mcf,

        -- C7+ (Heptanes plus) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volc7liq') }}::float as c7_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volc7gaseq') }}::float as c7_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volc7gas') }}::float as c7_gas_volume_mcf,

        -- N2 (Nitrogen) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('voln2liq') }}::float as n2_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('voln2gaseq') }}::float as n2_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('voln2gas') }}::float as n2_gas_volume_mcf,

        -- CO2 (Carbon dioxide) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volco2liq') }}::float as co2_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volco2gaseq') }}::float as co2_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volco2gas') }}::float as co2_gas_volume_mcf,

        -- H2S (Hydrogen sulfide) component volumes (converted to US units)
        {{ pv_cbm_to_bbl('volh2sliq') }}::float as h2s_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volh2sgaseq') }}::float as h2s_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volh2sgas') }}::float as h2s_gas_volume_mcf,

        -- Other components volumes (converted to US units)
        {{ pv_cbm_to_bbl('volothercompliq') }}::float as other_components_liquid_volume_bbl,
        {{ pv_cbm_to_mcf('volothercompgaseq') }}::float as other_components_gas_equivalent_mcf,
        {{ pv_cbm_to_mcf('volothercompgas') }}::float as other_components_gas_volume_mcf,

        -- heat content (converted to US units)
        {{ pv_joules_to_mmbtu('heat') }}::float as heat_content_mmbtu,

        -- calculation set reference
        trim(idreccalcset)::varchar as calc_settings_id,
        trim(idreccalcsettk)::varchar as calc_settings_table,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as daily_disposition_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        daily_disposition_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        disposition_date,
        disposition_year,
        disposition_month,
        day_of_month,

        -- unit and completion references
        unit_id,
        unit_table,
        completion_id,
        completion_table,
        reporting_contact_interval_id,
        reporting_contact_interval_table,

        -- outlet and disposition references
        outlet_send_id,
        outlet_send_table,
        disposition_unit_node_id,
        disposition_unit_node_table,
        disposition_unit_id,
        disposition_unit_table,

        -- total fluid volumes
        hcliq_volume_bbl,
        hcliq_gas_equivalent_mcf,
        gas_volume_mcf,
        water_volume_bbl,
        sand_volume_bbl,

        -- C1 (Methane) component volumes
        c1_liquid_volume_bbl,
        c1_gas_equivalent_mcf,
        c1_gas_volume_mcf,

        -- C2 (Ethane) component volumes
        c2_liquid_volume_bbl,
        c2_gas_equivalent_mcf,
        c2_gas_volume_mcf,

        -- C3 (Propane) component volumes
        c3_liquid_volume_bbl,
        c3_gas_equivalent_mcf,
        c3_gas_volume_mcf,

        -- iC4 (Iso-butane) component volumes
        ic4_liquid_volume_bbl,
        ic4_gas_equivalent_mcf,
        ic4_gas_volume_mcf,

        -- nC4 (Normal butane) component volumes
        nc4_liquid_volume_bbl,
        nc4_gas_equivalent_mcf,
        nc4_gas_volume_mcf,

        -- iC5 (Iso-pentane) component volumes
        ic5_liquid_volume_bbl,
        ic5_gas_equivalent_mcf,
        ic5_gas_volume_mcf,

        -- nC5 (Normal pentane) component volumes
        nc5_liquid_volume_bbl,
        nc5_gas_equivalent_mcf,
        nc5_gas_volume_mcf,

        -- C6 (Hexanes) component volumes
        c6_liquid_volume_bbl,
        c6_gas_equivalent_mcf,
        c6_gas_volume_mcf,

        -- C7+ (Heptanes plus) component volumes
        c7_liquid_volume_bbl,
        c7_gas_equivalent_mcf,
        c7_gas_volume_mcf,

        -- N2 (Nitrogen) component volumes
        n2_liquid_volume_bbl,
        n2_gas_equivalent_mcf,
        n2_gas_volume_mcf,

        -- CO2 (Carbon dioxide) component volumes
        co2_liquid_volume_bbl,
        co2_gas_equivalent_mcf,
        co2_gas_volume_mcf,

        -- H2S (Hydrogen sulfide) component volumes
        h2s_liquid_volume_bbl,
        h2s_gas_equivalent_mcf,
        h2s_gas_volume_mcf,

        -- Other components volumes
        other_components_liquid_volume_bbl,
        other_components_gas_equivalent_mcf,
        other_components_gas_volume_mcf,

        -- heat content
        heat_content_mmbtu,

        -- calculation set reference
        calc_settings_id,
        calc_settings_table,

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
