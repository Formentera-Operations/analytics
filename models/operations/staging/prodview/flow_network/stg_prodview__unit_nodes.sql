{{
    config(
        materialized='view',
        tags=['prodview', 'staging', 'formentera']
    )
}}

with

source as (
    select * from {{ source('prodview', 'PVT_PVUNITNODE') }}
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

        -- node configuration
        trim(name)::varchar as node_name,
        trim(typ)::varchar as node_type,
        dttmstart::timestamp_ntz as start_date,
        dttmend::timestamp_ntz as end_date,

        -- fluid and component properties
        trim(component)::varchar as component_name,
        trim(componentphase)::varchar as component_phase,
        trim(desfluid)::varchar as designated_fluid,
        keepwhole::boolean as keep_whole,
        trim(typfluidbaserestrict)::varchar as fluid_base_restriction_type,

        -- flow diagram and sorting
        sortflowdiag::float as flow_diagram_sort_order,

        -- migration tracking
        trim(keymigrationsource)::varchar as migration_source_key,
        trim(typmigrationsource)::varchar as migration_source_type,

        -- external IDs and corrections
        trim(otherid)::varchar as other_id,
        trim(correctionid1)::varchar as correction_id_1,
        trim(correctiontyp1)::varchar as correction_type_1,
        trim(correctionid2)::varchar as correction_id_2,
        trim(correctiontyp2)::varchar as correction_type_2,

        -- product and facility information
        trim(facproductname)::varchar as facility_product_name,
        usevirutalanalysis::boolean as use_virtual_analysis,

        -- disposition configuration
        dispositionpoint::boolean as disposition_point,
        trim(dispproductname)::varchar as disposition_product_name,
        trim(typdisp1)::varchar as disposition_type_1,
        trim(typdisp2)::varchar as disposition_type_2,
        trim(typdisphcliq)::varchar as hcliq_disposition_type,
        trim(dispida)::varchar as disposition_id_a,
        trim(dispidb)::varchar as disposition_id_b,

        -- purchaser information
        trim(purchasername)::varchar as purchaser_name,
        trim(purchasercode1)::varchar as purchaser_code_1,
        trim(purchasercode2)::varchar as purchaser_code_2,

        -- general configuration
        trim(com)::varchar as comments,
        dttmhide::timestamp_ntz as hide_record_date,
        trim(reportgroup)::varchar as report_group,
        ingathered::boolean as is_in_gathered,

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
        {{ dbt_utils.generate_surrogate_key(['id_rec']) }} as unit_node_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        unit_node_sk,

        -- identifiers
        id_rec,
        id_rec_parent,
        id_flownet,

        -- node configuration
        node_name,
        node_type,
        start_date,
        end_date,

        -- fluid and component properties
        component_name,
        component_phase,
        designated_fluid,
        keep_whole,
        fluid_base_restriction_type,

        -- flow diagram and sorting
        flow_diagram_sort_order,

        -- migration tracking
        migration_source_key,
        migration_source_type,

        -- external IDs and corrections
        other_id,
        correction_id_1,
        correction_type_1,
        correction_id_2,
        correction_type_2,

        -- product and facility information
        facility_product_name,
        use_virtual_analysis,

        -- disposition configuration
        disposition_point,
        disposition_product_name,
        disposition_type_1,
        disposition_type_2,
        hcliq_disposition_type,
        disposition_id_a,
        disposition_id_b,

        -- purchaser information
        purchaser_name,
        purchaser_code_1,
        purchaser_code_2,

        -- general configuration
        comments,
        hide_record_date,
        report_group,
        is_in_gathered,

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
