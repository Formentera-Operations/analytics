{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITREGBODYKEY') }}
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

        -- key information
        trim(keyname)::varchar as key_name,

        -- item references
        trim(idrecitem)::varchar as applies_to_item_id,
        trim(idrecitemtk)::varchar as applies_to_item_table,

        -- date range
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,

        -- type classifications
        trim(typ1)::varchar as key_type,
        trim(typ2)::varchar as key_sub_type,

        -- key values
        trim(keyvalue1)::varchar as key_value_1,
        trim(keyvalue2)::varchar as key_value_2,
        trim(keyvalue3)::varchar as key_value_3,

        -- key numbers
        keynum1::float as key_number_1,
        keynum2::float as key_number_2,
        keynum3::float as key_number_3,

        -- general information
        trim(com)::varchar as comments,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as regulatory_reporting_key_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        regulatory_reporting_key_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- key information
        key_name,

        -- item references
        applies_to_item_id,
        applies_to_item_table,

        -- date range
        start_date,
        end_date,

        -- type classifications
        key_type,
        key_sub_type,

        -- key values
        key_value_1,
        key_value_2,
        key_value_3,

        -- key numbers
        key_number_1,
        key_number_2,
        key_number_3,

        -- general information
        comments,

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
