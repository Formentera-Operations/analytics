{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITAGREEMT') }}
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

        -- agreement details
        trim(des)::varchar as agreement_description,
        trim(typ1)::varchar as agreement_type,
        trim(subtyp1)::varchar as agreement_subtype_1,
        trim(subtyp2)::varchar as agreement_subtype_2,

        -- agreement period
        dttmstart::timestamp_ntz as agreement_start_date,
        dttmend::timestamp_ntz as agreement_end_date,

        -- agreement application
        trim(idrecappliesto)::varchar as applies_to_id,
        trim(idrecappliestotk)::varchar as applies_to_table,

        -- reference IDs
        trim(refida)::varchar as wi_partner,
        trim(refidb)::varchar as reference_id_b,
        trim(refidc)::varchar as reference_id_c,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as partnership_agreement_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        partnership_agreement_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- agreement details
        agreement_description,
        agreement_type,
        agreement_subtype_1,
        agreement_subtype_2,

        -- agreement period
        agreement_start_date,
        agreement_end_date,

        -- agreement application
        applies_to_id,
        applies_to_table,

        -- reference IDs
        wi_partner,
        reference_id_b,
        reference_id_c,

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
