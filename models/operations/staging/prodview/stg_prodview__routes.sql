{{
    config(
        materialized='view',
        tags=['prodview', 'routes', 'configuration', 'staging']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVROUTESETROUTE') }}
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

        -- route information
        trim(name)::varchar as route_name,
        trim(com)::varchar as notes,

        -- user-defined fields
        trim(usertxt1)::varchar as foreman,
        trim(usertxt2)::varchar as primary_lease_operator,
        trim(usertxt3)::varchar as backup_lease_operator,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as route_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        route_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- route information
        route_name,
        notes,

        -- user-defined fields
        foreman,
        primary_lease_operator,
        backup_lease_operator,

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
