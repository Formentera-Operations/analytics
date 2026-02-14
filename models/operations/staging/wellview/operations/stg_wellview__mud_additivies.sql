{{
    config(
        materialized='view',
        tags=['wellview', 'staging', 'operations']
    )
}}

with

source as (
    select * from {{ source('wellview_calcs', 'WVT_WVJOBMUDADD') }}
    qualify 1 = row_number() over (
        partition by idrec
        order by _fivetran_synced desc
    )
),

renamed as (
    select
        -- identifiers
        trim(idrec)::varchar as mud_additive_id,
        trim(idrecparent)::varchar as job_id,
        trim(idwell)::varchar as well_id,

        -- additive identification
        trim(des)::varchar as description,
        trim(typ)::varchar as additive_type,
        trim(unitlabel)::varchar as unit_label,
        unitsz::float as unit_size,
        trim(note)::varchar as notes,

        -- vendor information
        trim(vendor)::varchar as vendor,
        trim(vendorcode)::varchar as vendor_code,
        trim(vendorsubcode)::varchar as vendor_subcode,

        -- cost codes and description
        trim(codedes)::varchar as code_description,
        trim(code1)::varchar as code_1,
        trim(code2)::varchar as code_2,
        trim(code3)::varchar as code_3,
        trim(code4)::varchar as code_4,
        trim(code5)::varchar as code_5,
        trim(code6)::varchar as code_6,

        -- cost information
        cost::float as unit_cost,
        costcalc::float as total_field_estimate_cost,

        -- consumption planning and tracking
        consumedesign::float as planned_consumed_amount,
        consumedcalc::float as total_consumed,
        consumedesignvarcalc::float as planned_vs_actual_consumed_variance,

        -- consumption per depth (converted from per-meter to per-foot)
        {{ wv_per_meter_to_per_foot('consumedperdepthcalc') }} as consumed_per_depth_per_ft,

        -- inventory tracking
        receivedcalc::float as total_received,
        returnedcalc::float as total_returned,
        inventorycalc::float as inventory_on_location,

        -- system / audit
        trim(syscreateuser)::varchar as created_by,
        syscreatedate::timestamp_ntz as created_at,
        trim(sysmoduser)::varchar as modified_by,
        sysmoddate::timestamp_ntz as modified_at,
        trim(systag)::varchar as system_tag,
        syslockdate::timestamp_ntz as system_lock_date,
        syslockme::boolean as system_lock_me,
        syslockchildren::boolean as system_lock_children,
        syslockmeui::boolean as system_lock_me_ui,
        syslockchildrenui::boolean as system_lock_children_ui,

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
        and mud_additive_id is not null
),

enhanced as (
    select
        {{ dbt_utils.generate_surrogate_key(['mud_additive_id']) }} as mud_additive_sk,
        *,
        current_timestamp() as _loaded_at
    from filtered
),

final as (
    select
        mud_additive_sk,

        -- identifiers
        mud_additive_id,
        job_id,
        well_id,

        -- additive identification
        description,
        additive_type,
        unit_label,
        unit_size,
        notes,

        -- vendor information
        vendor,
        vendor_code,
        vendor_subcode,

        -- cost codes and description
        code_description,
        code_1,
        code_2,
        code_3,
        code_4,
        code_5,
        code_6,

        -- cost information
        unit_cost,
        total_field_estimate_cost,

        -- consumption planning and tracking
        planned_consumed_amount,
        total_consumed,
        planned_vs_actual_consumed_variance,
        consumed_per_depth_per_ft,

        -- inventory tracking
        total_received,
        total_returned,
        inventory_on_location,

        -- system / audit
        created_by,
        created_at,
        modified_by,
        modified_at,
        system_tag,
        system_lock_date,
        system_lock_me,
        system_lock_children,
        system_lock_me_ui,
        system_lock_children_ui,

        -- dbt metadata
        _fivetran_deleted,
        _fivetran_synced,
        _loaded_at

    from enhanced
)

select * from final
