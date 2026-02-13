{{
    config(
        materialized='view',
        tags=['prodview', 'completions', 'targets', 'daily', 'staging']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPTARGET') }}
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
        dttmstart::date as target_start_date,

        -- target fields
        trim(typ)::varchar as target_type,
        usecalcdiff::boolean as is_use_in_diff_from_target_calculations,

        -- user-defined fields
        trim(usertxt1)::varchar as cc_forecast_name,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at_utc,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at_utc,
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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as production_target_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        production_target_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- dates
        target_start_date,

        -- target fields
        target_type,
        is_use_in_diff_from_target_calculations,

        -- user-defined fields
        cc_forecast_name,
        user_txt2,
        user_txt3,

        -- system / audit
        created_by,
        created_at_utc,
        modified_by,
        modified_at_utc,
        record_tag,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
