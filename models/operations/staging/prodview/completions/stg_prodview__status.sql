{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITCOMPSTATUS') }}
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

        -- status details
        dttm::timestamp_ntz as status_date,
        trim(status)::varchar as status,
        trim(primaryfluidtyp)::varchar as primary_fluid_type,
        trim(flowdirection)::varchar as flow_direction,
        trim(commingled)::varchar as commingled,
        trim(typfluidprod)::varchar as oil_or_condensate,

        -- completion characteristics
        trim(typcompletion)::varchar as completion_type,
        trim(methodprod)::varchar as production_method,

        -- calculation and reporting flags
        trim(calclostprod)::varchar as calc_lost_production,
        trim(wellcountincl)::varchar as include_in_well_count,

        -- comments
        trim(com)::varchar as comments,

        -- user-defined fields
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,
        usernum1::float as user_num1,
        usernum2::float as user_num2,
        usernum3::float as user_num3,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as status_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        status_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- status details
        status_date,
        status,
        primary_fluid_type,
        flow_direction,
        commingled,
        oil_or_condensate,

        -- completion characteristics
        completion_type,
        production_method,

        -- calculation and reporting flags
        calc_lost_production,
        include_in_well_count,

        -- comments
        comments,

        -- user-defined fields
        user_txt1,
        user_txt2,
        user_txt3,
        user_num1,
        user_num2,
        user_num3,

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
