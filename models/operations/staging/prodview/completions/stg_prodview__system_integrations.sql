{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVSYSINTEGRATION') }}
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
        trim(tblkeyparent)::varchar as table_key_parent,

        -- integration system information
        trim(integratordes)::varchar as integrator_description,
        trim(integratorver)::varchar as integrator_version,
        trim(afproduct)::varchar as product_description,

        -- external system references
        trim(afidentity)::varchar as af_id_entity,
        trim(afidrec)::varchar as af_id_rec,

        -- notes
        trim(note)::varchar as note,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as system_integration_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        system_integration_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,
        table_key_parent,

        -- integration system information
        integrator_description,
        integrator_version,
        product_description,

        -- external system references
        af_id_entity,
        af_id_rec,

        -- notes
        note,

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
