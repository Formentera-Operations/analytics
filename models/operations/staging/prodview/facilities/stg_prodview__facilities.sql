{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVFACILITY') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as id_rec,
        trim(idflownet)::varchar as id_flownet,

        -- facility EID and name
        trim(idpa)::varchar as facility_eid,
        trim(name)::varchar as facility_name,
        trim(permanentid)::varchar as permanent_facility_id,

        -- facility type classifiers
        trim(typpa)::varchar as facility_type_pa,
        trim(typ1)::varchar as facility_type_1,
        trim(typ2)::varchar as facility_type_2,
        trim(typregulatory)::varchar as facility_type_regulatory,

        -- user-defined facility identifiers
        trim(facilityida)::varchar as facility_id_a,
        trim(facilityidb)::varchar as facility_id_b,
        trim(facilityidc)::varchar as facility_id_c,
        trim(facilityidd)::varchar as facility_id_d,

        -- primary unit link
        trim(idrecunitprimary)::varchar as primary_unit_id_rec,
        trim(idrecunitprimarytk)::varchar as primary_unit_table_key,

        -- responsible party references
        trim(idrecresp1)::varchar as responsible_party_1_id_rec,
        trim(idrecresp1tk)::varchar as responsible_party_1_table_key,
        trim(idrecresp2)::varchar as responsible_party_2_id_rec,
        trim(idrecresp2tk)::varchar as responsible_party_2_table_key,

        -- lifecycle dates
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,
        dttmhide::timestamp_ntz as hide_date,

        -- display / reporting flags
        hidefacrev::boolean as is_revenue_hidden,
        treathcliquidasgas::boolean as is_hcliq_treated_as_gas,

        -- comments and user-defined text
        trim(com)::varchar as comments,
        trim(usertxt1)::varchar as user_txt1,
        trim(usertxt2)::varchar as user_txt2,
        trim(usertxt3)::varchar as user_txt3,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as facility_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        facility_sk,

        -- identifiers
        id_rec,
        id_flownet,

        -- facility EID and name
        facility_eid,
        facility_name,
        permanent_facility_id,

        -- facility type classifiers
        facility_type_pa,
        facility_type_1,
        facility_type_2,
        facility_type_regulatory,

        -- user-defined facility identifiers
        facility_id_a,
        facility_id_b,
        facility_id_c,
        facility_id_d,

        -- primary unit link
        primary_unit_id_rec,
        primary_unit_table_key,

        -- responsible party references
        responsible_party_1_id_rec,
        responsible_party_1_table_key,
        responsible_party_2_id_rec,
        responsible_party_2_table_key,

        -- lifecycle dates
        start_date,
        end_date,
        hide_date,

        -- display / reporting flags
        is_revenue_hidden,
        is_hcliq_treated_as_gas,

        -- comments and user-defined text
        comments,
        user_txt1,
        user_txt2,
        user_txt3,

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
